import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from ddgs import DDGS
except ImportError:
    DDGS = None


class DDGSSourceAdapter:
    """
    Lightweight search-engine discovery.

    Directory/profile URLs are kept as source_links.
    They are NOT falsely labelled as the business's own website.
    """

    def __init__(self, source_name, domains):
        self.source_name = source_name
        self.domains = list(domains)
        self.cancel_requested = False
        self.pause_event = threading.Event()
        self.pause_event.set()

    def cancel(self):
        self.cancel_requested = True
        self.pause_event.set()

    def pause(self):
        self.pause_event.clear()

    def resume(self):
        self.pause_event.set()

    def _wait_if_paused(self):
        while not self.cancel_requested and not self.pause_event.is_set():
            self.pause_event.wait(timeout=0.2)

    def search(
        self,
        keyword,
        country,
        city,
        state,
        zip_code,
        limit=25,
        progress_cb=None,
        lead_cb=None,
    ):
        self.cancel_requested = False
        self.pause_event.set()

        if DDGS is None:
            raise RuntimeError("DDGS is not installed. Run: python -m pip install ddgs")

        location = " ".join(
            value for value in [city, state, zip_code, country] if value
        ).strip()

        queries = []
        for domain in self.domains:
            base = f'site:{domain} "{keyword}"'

            # Every query remains location-scoped. There is NO generic
            # nationwide fallback because that can introduce out-of-area leads.
            if city and state:
                queries.append(f'{base} "{city}" "{state}"')
                queries.append(f'{base} "{city}, {state}"')
            elif city:
                queries.append(f'{base} "{city}"')
            elif state:
                queries.append(f'{base} "{state}"')
            elif zip_code:
                queries.append(f'{base} "{zip_code}"')
            elif country:
                queries.append(f'{base} "{country}"')

            if location:
                queries.append(f'{base} "{location}"')

        queries = list(dict.fromkeys(q for q in queries if q.strip()))
        if not queries:
            return []

        results = []
        seen_urls = set()
        result_lock = threading.RLock()
        completed_queries = 0

        # DDGS variants are independent network searches. Running a few in
        # parallel removes the old query-1 -> query-2 -> query-3 waiting chain.
        query_workers = min(4, len(queries))

        def fetch_query(query):
            if self.cancel_requested:
                return []

            self._wait_if_paused()
            if self.cancel_requested:
                return []

            try:
                # Do not massively over-fetch each query; multiple variants are
                # now running concurrently and deduplication happens below.
                per_query = min(
                    max(int(limit * 1.35), 15),
                    60,
                )
                return list(
                    DDGS().text(
                        query,
                        max_results=per_query,
                    )
                )
            except Exception:
                return []

        executor = ThreadPoolExecutor(
            max_workers=query_workers,
            thread_name_prefix=f"ddgs-{self.source_name.lower().replace(' ', '-')}",
        )

        future_map = {
            executor.submit(fetch_query, query): query
            for query in queries
        }

        try:
            for future in as_completed(future_map):
                if self.cancel_requested or len(results) >= limit:
                    break

                self._wait_if_paused()
                if self.cancel_requested:
                    break

                try:
                    rows = future.result()
                except Exception:
                    rows = []

                completed_queries += 1

                if progress_cb:
                    progress_cb(
                        min(
                            90,
                            int(completed_queries / max(1, len(queries)) * 90),
                        ),
                        f"Searching {self.source_name} in parallel...",
                    )

                for result in rows:
                    if self.cancel_requested:
                        break

                    href = str(
                        result.get("href", result.get("url", "")) or ""
                    ).strip()

                    if not href:
                        continue

                    with result_lock:
                        if href in seen_urls or len(results) >= limit:
                            continue

                        if not self._matches_expected_domain(href):
                            continue

                        seen_urls.add(href)

                    title = str(result.get("title", "") or "").strip()
                    body = str(
                        result.get("body", result.get("description", "")) or ""
                    ).strip()

                    if not self._is_geographically_relevant(
                        title=title,
                        body=body,
                        target_city=city,
                        target_state=state,
                        target_zip=zip_code,
                        target_country=country,
                    ):
                        continue

                    name = self._clean_title(title)
                    if not name:
                        continue

                    linkedin = (
                        href
                        if self.source_name == "LinkedIn"
                        else ""
                    )

                    lead = {
                        "name": name,
                        "keyword": keyword,
                        "city": city,
                        "state": state,
                        "country": country or "USA",
                        "zip_code": zip_code,
                        "phone": self._extract_phone(f"{title} {body}"),
                        "website": "",
                        "email": self._extract_email(f"{title} {body}"),
                        "address": self._extract_address_hint(body),
                        "rating": "",
                        "reviews": "",
                        "source": self.source_name,
                        "sources": self.source_name,
                        "linkedin": linkedin,
                        "facebook": "",
                        "instagram": "",
                        "notes": body[:700],
                        "source_links": [href],
                        "google_maps_url": "",
                        "latitude": "",
                        "longitude": "",
                    }

                    with result_lock:
                        if len(results) >= limit:
                            break
                        results.append(lead)

                    if lead_cb:
                        lead_cb(lead)

                if len(results) >= limit:
                    self.cancel_requested = True
                    break

        finally:
            # Cancel queued variants immediately once enough candidates exist.
            for future in future_map:
                if not future.done():
                    future.cancel()

            executor.shutdown(
                wait=False,
                cancel_futures=True,
            )

        # reset local cancellation flag so the adapter can be reused next search
        reached_limit = len(results) >= limit
        self.cancel_requested = False

        if progress_cb:
            progress_cb(
                100,
                f"{self.source_name} found {len(results)} candidates",
            )

        return results[:limit]

    def _is_geographically_relevant(
        self,
        title,
        body,
        target_city,
        target_state,
        target_zip,
        target_country,
    ):
        """
        Keep neutral snippets from an already location-scoped DDGS query, but
        reject strong evidence that the result belongs somewhere else.
        """
        haystack = f"{title} {body}".strip()
        lower = haystack.lower()

        city = str(target_city or "").strip().lower()
        state = str(target_state or "").strip().upper()
        zipcode = str(target_zip or "").strip()
        country = str(target_country or "").strip().lower()

        # Clear positive evidence.
        if city and city in lower:
            return True
        if state and re.search(rf"\b{re.escape(state)}\b", haystack, re.I):
            return True
        if zipcode and zipcode in haystack:
            return True

        # Country conflicts for USA searches.
        usa_names = {
            "usa", "us", "u.s.", "united states",
            "united states of america",
        }
        if country in usa_names:
            foreign_markers = {
                "canada", "canadian", "united kingdom", "australia",
                "india", "pakistan", "germany", "france", "mexico",
                "singapore", "uae", "united arab emirates",
            }
            for marker in foreign_markers:
                if re.search(rf"\b{re.escape(marker)}\b", lower):
                    return False

        # Detect strong conflicting US-state abbreviations in title/snippet.
        state_abbrs = {
            "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID",
            "IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS",
            "MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
            "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV",
            "WI","WY","DC",
        }

        found_states = set()
        for match in re.finditer(
            r"(?:,\s*|\s)([A-Z]{2})(?=\s|\d{5}\b|$|[,.;|()/-])",
            haystack,
        ):
            code = match.group(1).upper()
            if code in state_abbrs:
                found_states.add(code)

        if state and found_states and state not in found_states:
            return False

        # Explicit city/state examples that frequently appear in indexed titles.
        conflict_phrases = [
            "chicago il", "new york ny", "los angeles ca", "miami fl",
            "denver co", "phoenix az", "seattle wa", "boston ma",
            "atlanta ga", "dallas tx", "houston tx", "san antonio tx",
        ]
        for phrase in conflict_phrases:
            if phrase in lower and (not city or city not in phrase):
                return False

        # Low-value nationwide directory/index pages are not local leads.
        generic_bad = [
            "small business hub united states",
            "business directory united states",
            "nationwide business directory",
        ]
        if any(marker in lower for marker in generic_bad):
            return False

        # Neutral result: allowed because every DDGS query is already
        # constrained to the requested location.
        return True

    def _matches_expected_domain(self, url):
        lower = str(url or "").lower()
        return any(domain.lower() in lower for domain in self.domains)

    def _clean_title(self, title):
        title = str(title or "").strip()
        if not title:
            return ""

        for separator in [
            " | LinkedIn",
            " - LinkedIn",
            " | Yellow Pages",
            " - Yellow Pages",
            " | Hotfrog",
            " - Hotfrog",
        ]:
            if separator in title:
                title = title.split(separator, 1)[0].strip()

        return title[:180]

    def _extract_phone(self, text):
        for pattern in [
            r"\+1[\s.\-]?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}",
            r"\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}",
        ]:
            match = re.search(pattern, text)
            if match:
                return match.group(0).strip()
        return ""

    def _extract_email(self, text):
        match = re.search(
            r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
            text,
        )
        return match.group(0).strip() if match else ""

    def _extract_address_hint(self, text):
        pattern = (
            r"\b\d{1,6}\s+"
            r"[A-Za-z0-9 .'\-]{3,70}"
            r"(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|"
            r"Drive|Dr|Lane|Ln|Highway|Hwy|Court|Ct|Way|Parkway|Pkwy)"
            r"\b[^,.]*"
        )
        match = re.search(pattern, text, flags=re.I)
        return match.group(0).strip() if match else ""
