import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse

from .http_provider import HTTPProvider


EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(
    r"(?:\+?1[\s.\-]?)?(?:\(\d{3}\)|\d{3})[\s.\-]\d{3}[\s.\-]\d{4}"
)

SOCIAL_DOMAINS = {
    "linkedin": "linkedin.com/",
    "facebook": "facebook.com/",
    "instagram": "instagram.com/",
}

DIRECTORY_DOMAINS = {
    "hotfrog.com",
    "yellowpages.com",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "yelp.com",
    "google.com",
    "bing.com",
    "x.com",
    "twitter.com",
}


class WebsiteEnricher:
    """
    Fast lead enrichment optimized for the USA Lead Finder.

    Pipeline:
      discovered profile -> business homepage -> contact/about pages

    Scrapling is the primary fetcher through HTTPProvider. httpx remains the
    fallback. Contact/about pages are fetched concurrently only when useful.

    Internal "Bulk" mode is intentionally shallow:
      profile -> homepage only
    It never crawls contact/about pages during large 151+ lead searches.
    """

    def __init__(self, timeout=7.0, page_workers=3):
        self.http = HTTPProvider(timeout=timeout)
        self.page_workers = max(2, min(4, int(page_workers)))

    def enrich(self, lead, mode="Balanced"):
        result = dict(lead or {})
        mode = str(mode or "Balanced").strip().title()
        source_links = self._normalise_links(result.get("source_links"))

        profile_url = source_links[0] if source_links else ""
        website = str(result.get("website") or "").strip()

        # FAST MODE:
        # If a lead already has the two most valuable operational fields,
        # do not spend extra network time re-fetching it.
        if (
            mode in {"Fast", "Bulk"}
            and result.get("phone")
            and website
        ):
            return result

        # Directory/search profile first only when it can still add useful data.
        # This is especially important for DDGS-discovered Hotfrog/YP/LinkedIn
        # rows where the profile can reveal the real business website.
        if profile_url and (
            not website
            or not result.get("phone")
            or (mode not in {"Fast", "Bulk"} and not result.get("email"))
        ):
            page = self.http.fetch(profile_url)
            if page:
                profile_data = self._extract_page(page["url"], page["text"])
                self._fill_missing(result, profile_data)
                website = str(result.get("website") or website or "").strip()

        # Business homepage.
        # In Fast mode we only fetch it if phone/email data is still incomplete.
        should_fetch_home = bool(website) and (
            mode not in {"Fast", "Bulk"}
            or not result.get("phone")
            or not result.get("email")
        )

        if should_fetch_home:
            home = self.http.fetch(website)
            if home:
                home_data = self._extract_page(home["url"], home["text"])
                before = {
                    field: result.get(field)
                    for field in (
                        "phone", "email", "address",
                        "linkedin", "facebook", "instagram",
                    )
                }
                self._fill_missing(result, home_data)

                if any(
                    not before.get(field) and result.get(field)
                    for field in before
                ):
                    self._append_source(result, "Website")

                # Bulk mode ends after homepage/schema/tel/mailto extraction.
                # It avoids /contact and /about crawling for 151+ lead jobs.
                if mode != "Bulk" and self._needs_more_contact_data(result):
                    candidates = self._candidate_pages(
                        home["url"],
                        home_data.get("candidate_urls", []),
                    )

                    # Fast mode checks only ONE highest-value internal page.
                    # Balanced/Deep may check up to 3 in parallel.
                    max_pages = 1 if mode == "Fast" else 3
                    self._enrich_candidate_pages(
                        result,
                        candidates,
                        max_pages=max_pages,
                    )

        return result

    @staticmethod
    def _normalise_links(value):
        if isinstance(value, str):
            return [x.strip() for x in value.splitlines() if x.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(x).strip() for x in value if str(x).strip()]
        return []

    @staticmethod
    def _append_source(result, source_name):
        source_name = str(source_name or "").strip()
        if not source_name:
            return

        current = str(
            result.get("sources")
            or result.get("source")
            or ""
        )

        parts = [
            item.strip()
            for item in re.split(r"[,•|]", current)
            if item.strip()
        ]

        if source_name not in parts:
            parts.append(source_name)

        result["sources"] = ", ".join(parts)

    @staticmethod
    def _needs_more_contact_data(result):
        # Continue if either primary contact method is missing.
        return not result.get("email") or not result.get("phone")

    def _enrich_candidate_pages(self, result, urls, max_pages=3):
        if not urls:
            return

        max_pages = max(1, min(3, int(max_pages)))

        # Fast mode uses one page and avoids ThreadPoolExecutor overhead.
        if max_pages == 1:
            page = self.http.fetch(urls[0])
            if page:
                before = {
                    field: result.get(field)
                    for field in ("phone", "email", "address")
                }
                data = self._extract_page(page["url"], page["text"])
                self._fill_missing(result, data)
                if any(
                    not before.get(field) and result.get(field)
                    for field in before
                ):
                    self._append_source(result, "Website")
            return

        # Balanced/Deep can inspect up to 3 useful pages concurrently.
        with ThreadPoolExecutor(
            max_workers=min(self.page_workers, max_pages)
        ) as pool:
            futures = {
                pool.submit(self.http.fetch, url): url
                for url in urls[:max_pages]
            }

            for future in as_completed(futures):
                try:
                    page = future.result()
                except Exception:
                    page = None

                if not page:
                    continue

                before = {
                    field: result.get(field)
                    for field in ("phone", "email", "address")
                }
                data = self._extract_page(page["url"], page["text"])
                self._fill_missing(result, data)

                if any(
                    not before.get(field) and result.get(field)
                    for field in before
                ):
                    self._append_source(result, "Website")

                # Once both are found, remaining futures are no longer useful.
                if result.get("email") and result.get("phone"):
                    for pending in futures:
                        if not pending.done():
                            pending.cancel()
                    break

    def _candidate_pages(self, homepage_url, discovered):
        parsed = urlparse(homepage_url)
        base = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
        base_domain = self.http.domain(homepage_url)

        ordered = []

        def add(url):
            url = str(url or "").strip()
            if not url:
                return
            if self.http.domain(url) != base_domain:
                return
            if url.rstrip("/") == homepage_url.rstrip("/"):
                return
            if url not in ordered:
                ordered.append(url)

        # Prefer discovered CONTACT links first, then ABOUT links.
        discovered = [str(url or "").strip() for url in discovered if str(url or "").strip()]
        for url in discovered:
            if "contact" in url.lower():
                add(url)

        for path in ("/contact", "/contact-us"):
            add(base.rstrip("/") + path)

        for url in discovered:
            if "contact" not in url.lower():
                add(url)

        add(base.rstrip("/") + "/about")

        return ordered[:3]

    def _fill_missing(self, target, incoming):
        for field in [
            "phone",
            "email",
            "website",
            "address",
            "linkedin",
            "facebook",
            "instagram",
        ]:
            if not target.get(field) and incoming.get(field):
                target[field] = incoming[field]

        links = self._normalise_links(target.get("source_links"))
        for link in incoming.get("source_links", []):
            if link and link not in links:
                links.append(link)
        target["source_links"] = links

    @staticmethod
    def _jsonld_objects(soup):
        objects = []

        for script in soup.find_all(
            "script",
            attrs={"type": re.compile(r"application/ld\+json", re.I)},
        ):
            raw = script.string or script.get_text(" ", strip=True)
            raw = str(raw or "").strip()

            if not raw:
                continue

            try:
                parsed = json.loads(raw)
            except Exception:
                # Some sites leave harmless trailing data around JSON-LD.
                start = min(
                    [i for i in (raw.find("{"), raw.find("[")) if i >= 0],
                    default=-1,
                )
                if start < 0:
                    continue

                try:
                    parsed = json.loads(raw[start:])
                except Exception:
                    continue

            stack = (
                list(parsed)
                if isinstance(parsed, list)
                else [parsed]
            )

            while stack:
                item = stack.pop()

                if not isinstance(item, dict):
                    continue

                objects.append(item)

                graph = item.get("@graph")
                if isinstance(graph, list):
                    stack.extend(
                        value
                        for value in graph
                        if isinstance(value, dict)
                    )

        return objects

    @staticmethod
    def _jsonld_address(value):
        if isinstance(value, str):
            return value.strip()

        if not isinstance(value, dict):
            return ""

        parts = [
            value.get("streetAddress"),
            value.get("addressLocality"),
            value.get("addressRegion"),
            value.get("postalCode"),
            value.get("addressCountry"),
        ]

        return ", ".join(
            str(part).strip()
            for part in parts
            if str(part or "").strip()
        )

    def _extract_jsonld(self, soup):
        best = {
            "phone": "",
            "email": "",
            "website": "",
            "address": "",
            "linkedin": "",
            "facebook": "",
            "instagram": "",
            "source_links": [],
        }

        preferred_types = {
            "localbusiness",
            "organization",
            "professionalservice",
            "plumber",
            "electrician",
            "homeandconstructionbusiness",
            "store",
            "corporation",
        }

        objects = self._jsonld_objects(soup)

        def object_score(obj):
            raw_type = obj.get("@type", "")
            types = (
                raw_type
                if isinstance(raw_type, list)
                else [raw_type]
            )
            normalized = {
                str(value or "").lower().replace(" ", "")
                for value in types
            }

            score = 0
            if normalized & preferred_types:
                score += 5
            if obj.get("telephone"):
                score += 2
            if obj.get("email"):
                score += 2
            if obj.get("address"):
                score += 2
            if obj.get("url"):
                score += 1
            return score

        objects.sort(key=object_score, reverse=True)

        for obj in objects:
            phone = str(
                obj.get("telephone")
                or obj.get("phone")
                or ""
            ).strip()

            email = str(obj.get("email") or "").strip()
            if email.lower().startswith("mailto:"):
                email = email.split(":", 1)[-1].strip()

            website = str(
                obj.get("url")
                or obj.get("@id")
                or ""
            ).strip()

            address = self._jsonld_address(obj.get("address"))

            if not best["phone"] and phone:
                best["phone"] = phone
            if not best["email"] and email:
                best["email"] = email
            if not best["website"] and website.startswith(("http://", "https://")):
                best["website"] = website
            if not best["address"] and address:
                best["address"] = address

            same_as = obj.get("sameAs", [])
            if isinstance(same_as, str):
                same_as = [same_as]

            for link in same_as if isinstance(same_as, list) else []:
                link = str(link or "").strip()
                lower = link.lower()

                if not link:
                    continue

                if "linkedin.com/" in lower and not best["linkedin"]:
                    best["linkedin"] = link
                elif "facebook.com/" in lower and not best["facebook"]:
                    best["facebook"] = link
                elif "instagram.com/" in lower and not best["instagram"]:
                    best["instagram"] = link

                if link not in best["source_links"]:
                    best["source_links"].append(link)

            if best["website"] and best["website"] not in best["source_links"]:
                best["source_links"].append(best["website"])

        return best

    def _extract_page(self, page_url, html):
        soup = self.http.soup(html)
        if soup is None:
            return {}

        text = " ".join(soup.stripped_strings)
        structured = self._extract_jsonld(soup)

        email = str(structured.get("email") or "").strip()
        mailto = soup.select_one('a[href^="mailto:"]')
        if mailto and not email:
            email = (
                mailto.get("href", "")
                .split(":", 1)[-1]
                .split("?", 1)[0]
                .strip()
            )
        if not email:
            match = EMAIL_RE.search(text)
            if match:
                email = match.group(0)

        phone = str(structured.get("phone") or "").strip()
        tel = soup.select_one('a[href^="tel:"]')
        if tel and not phone:
            phone = tel.get("href", "").split(":", 1)[-1].strip()
        if not phone:
            match = PHONE_RE.search(text)
            if match:
                phone = match.group(0)

        page_domain = self.http.domain(page_url)
        website = str(structured.get("website") or "").strip()
        contact_url = ""
        candidate_urls = []
        social = {
            "linkedin": str(structured.get("linkedin") or "").strip(),
            "facebook": str(structured.get("facebook") or "").strip(),
            "instagram": str(structured.get("instagram") or "").strip(),
        }
        external = []

        for anchor in soup.find_all("a", href=True):
            href = self.http.absolute(page_url, anchor.get("href", "").strip())
            if not href:
                continue

            lower = href.lower()
            label = " ".join(anchor.stripped_strings).strip().lower()
            host = self.http.domain(href)

            for field, needle in SOCIAL_DOMAINS.items():
                if needle in lower and not social[field]:
                    social[field] = href

            # Same-domain high-value pages for email/contact discovery.
            if host == page_domain and (
                "contact" in label
                or "contact" in lower
                or "about" in label
                or "/about" in lower
            ):
                if href not in candidate_urls:
                    candidate_urls.append(href)
                if not contact_url and "contact" in lower:
                    contact_url = href

            # On directory/profile pages this usually identifies the real site.
            if host and host != page_domain:
                if not any(
                    host == blocked or host.endswith("." + blocked)
                    for blocked in DIRECTORY_DOMAINS
                ):
                    if href not in external:
                        external.append(href)

        if external and (
            not website
            or self.http.domain(website) == page_domain
        ):
            website = external[0]

        source_links = list(structured.get("source_links") or [])
        for link in [
            page_url,
            website,
            social["linkedin"],
            social["facebook"],
            social["instagram"],
            contact_url,
        ]:
            if link and link not in source_links:
                source_links.append(link)

        return {
            "phone": phone,
            "email": email,
            "website": website,
            "address": (
                str(structured.get("address") or "").strip()
                or self._address_hint(text)
            ),
            "linkedin": social["linkedin"],
            "facebook": social["facebook"],
            "instagram": social["instagram"],
            "contact_url": contact_url,
            "candidate_urls": candidate_urls,
            "source_links": source_links,
        }

    @staticmethod
    def _address_hint(text):
        pattern = re.compile(
            r"\b\d{1,6}\s+"
            r"[A-Za-z0-9 .'\-]{3,80}"
            r"(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|"
            r"Lane|Ln|Highway|Hwy|Court|Ct|Way|Parkway|Pkwy)"
            r"\b[^|•\n]{0,60}",
            re.I,
        )
        match = pattern.search(text or "")
        return match.group(0).strip(" ,.-") if match else ""
