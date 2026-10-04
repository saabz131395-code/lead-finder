import math
import re
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, as_completed
from urllib.parse import urlparse
from difflib import SequenceMatcher

from .deduplicator import LeadDeduplicator
from .cache import SeenLeadCache
from providers import WebsiteEnricher


class HybridSourceEngine:
    """
    Concurrent discovery + persistent seen-cache + parallel enrichment.

    Repeated searches for the same keyword/location do not return leads that
    were already returned before. Cache keys are loaded into RAM once per
    search, so cache checks do not slow down scraping.
    """

    def __init__(
        self,
        adapters,
        max_workers=6,
        enrichment_workers=8,
        cache_max_rows=20000,
    ):
        self.adapters = dict(adapters)
        self.max_workers = max(1, int(max_workers))
        self.enrichment_workers = max(2, int(enrichment_workers))

        self.cache = SeenLeadCache(max_rows=cache_max_rows)
        self.enricher = WebsiteEnricher(timeout=7.0)

        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()

        self._user_stop = False
        self._target_reached = False

        self._lock = threading.RLock()
        self._source_counts = {}
        self._source_progress = {}
        self._source_done = set()
        self._dedup = LeadDeduplicator()

        self._selected = []
        self._target = 0
        self._min_google_maps = 0
        self._scope_key = ""
        self._seen_keys = set()

        # Automatic large-search mode (151+ requested leads).
        self._bulk_mode = False
        self._bulk_discovery_target = 0
        self._bulk_discovery_complete = False

    def stop(self, user_requested=True):
        if user_requested:
            self._user_stop = True

        self._stop_event.set()
        self._pause_event.set()

        for adapter in self.adapters.values():
            try:
                adapter.cancel()
            except Exception:
                pass

    cancel = stop

    def pause(self):
        if self._stop_event.is_set():
            return

        self._pause_event.clear()

        for adapter in self.adapters.values():
            try:
                adapter.pause()
            except Exception:
                pass

    def resume(self):
        if self._stop_event.is_set():
            return

        self._pause_event.set()

        for adapter in self.adapters.values():
            try:
                adapter.resume()
            except Exception:
                pass

    def _wait_if_paused(self):
        while not self._stop_event.is_set() and not self._pause_event.is_set():
            self._pause_event.wait(timeout=0.2)

    @staticmethod
    def _norm(value):
        return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

    @staticmethod
    def _domain(url):
        try:
            value = str(url or "").strip()
            if not value:
                return ""
            if "://" not in value:
                value = "https://" + value
            return urlparse(value).netloc.lower().replace("www.", "")
        except Exception:
            return ""

    def lead_key(self, lead):
        maps_url = str(lead.get("google_maps_url") or "").strip()
        if maps_url:
            return "maps:" + maps_url

        phone = re.sub(r"\D+", "", str(lead.get("phone") or ""))
        if phone:
            return "phone:" + phone

        domain = self._domain(lead.get("website"))
        if domain:
            return "domain:" + domain

        name = self._norm(lead.get("name"))
        address = self._norm(lead.get("address"))
        city = self._norm(lead.get("city"))

        if name and address:
            return f"name-address:{name}|{address}"

        if name and city:
            return f"name-city:{name}|{city}"

        return ""

    @staticmethod
    def quality_score(lead):
        score = 0
        if lead.get("phone"):
            score += 4
        if lead.get("email"):
            score += 4
        if lead.get("website"):
            score += 3
        if lead.get("address"):
            score += 3
        if lead.get("google_maps_url"):
            score += 3
        if lead.get("rating"):
            score += 1
        if lead.get("reviews"):
            score += 1
        if lead.get("linkedin"):
            score += 1
        if lead.get("facebook"):
            score += 1
        if lead.get("instagram"):
            score += 1
        return score

    @classmethod
    def is_qualified(cls, lead):
        return cls.quality_score(lead) >= 4

    def _qualified_count(self):
        return sum(1 for lead in self._dedup.snapshot() if self.is_qualified(lead))

    def _overall_progress(self):
        if not self._selected:
            return 0

        with self._lock:
            values = [
                self._source_progress.get(source, 0)
                for source in self._selected
            ]

        return min(95, int(sum(values) / max(1, len(values))))

    def _can_finish_target(self):
        if self._qualified_count() < self._target:
            return False

        if "Google Maps" not in self._selected:
            return True

        with self._lock:
            maps_count = self._source_counts.get("Google Maps", 0)
            maps_done = "Google Maps" in self._source_done

        if maps_done:
            return True

        return maps_count >= self._min_google_maps

    def _fresh_best_results(self, limit):
        rows = self._dedup.snapshot()

        fresh = []
        for row in rows:
            key = self.lead_key(row)
            if not key or key in self._seen_keys:
                continue
            fresh.append(row)

        fresh.sort(
            key=lambda row: (
                self.quality_score(row),
                bool(row.get("google_maps_url")),
                bool(row.get("phone")),
                bool(row.get("email")),
                bool(row.get("website")),
            ),
            reverse=True,
        )

        return fresh[:limit]

    def _keyword_rounds(self, keyword):
        """
        Conservative expansion only when the exact search cannot produce
        enough fresh leads. The original keyword always stays first.
        """
        keyword = str(keyword or "").strip()
        if not keyword:
            return [""]

        candidates = [
            keyword,
            f"{keyword} services",
            f"{keyword} company",
            f"{keyword} professionals",
            f"local {keyword}",
            f"{keyword} business",
        ]

        seen = set()
        rounds = []
        for item in candidates:
            key = item.lower().strip()
            if key and key not in seen:
                seen.add(key)
                rounds.append(item.strip())

        return rounds

    def _bulk_target_size(self):
        """
        Candidate pool needed before final enrichment.

        151+ searches deliberately over-discover first, then enrich only the
        final selected leads. This avoids enriching hundreds of candidates
        that would later be discarded as duplicates or lower-quality rows.
        """
        if not self._bulk_mode:
            return self._target

        buffer_size = max(
            40,
            math.ceil(self._target * 0.25),
        )
        return min(
            650,
            self._target + buffer_size,
        )

    def _cancel_discovery_adapters(self):
        """
        Stop source discovery without marking the whole search as stopped.
        Final bulk enrichment still needs to run afterwards.
        """
        for adapter in self.adapters.values():
            try:
                adapter.cancel()
            except Exception:
                pass

    def _bulk_candidate_count(self):
        return self._fresh_count()

    def _bulk_final_enrich(
        self,
        candidates,
        progress_cb,
        lead_update_cb=None,
    ):
        """
        Enrich ONLY the already-selected final candidates.

        Bulk mode deliberately avoids contact/about crawling and Maps
        cross-enrichment. WebsiteEnricher("Bulk") performs only:
          directory profile -> actual website -> homepage/schema/tel/mailto.
        """
        candidates = [dict(row) for row in candidates[: self._target]]

        if not candidates:
            return []

        # Native Maps leads are already rich. Also skip rows that already have
        # both phone + website because they are operationally useful.
        work = []
        for index, lead in enumerate(candidates):
            if lead.get("google_maps_url"):
                continue
            if lead.get("phone") and lead.get("website"):
                continue
            work.append((index, lead))

        if not work:
            return candidates

        # Large jobs benefit from more HTTP concurrency, but keep it bounded so
        # we do not overwhelm the PC or remote sites.
        workers = min(
            16,
            max(8, self.enrichment_workers + 4),
            len(work),
        )

        progress_cb(
            88,
            (
                f"Bulk final enrichment: {len(work)} of "
                f"{len(candidates)} selected leads"
            ),
        )

        completed = 0

        def enrich_one(index, lead):
            if self._user_stop:
                return index, lead

            try:
                enriched = self.enricher.enrich(
                    lead,
                    mode="Bulk",
                )
            except Exception:
                enriched = dict(lead)

            return index, enriched

        pool = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="bulk-final-enrich",
        )

        futures = {
            pool.submit(enrich_one, index, lead): index
            for index, lead in work
        }

        try:
            for future in as_completed(futures):
                if self._user_stop:
                    break

                try:
                    index, enriched = future.result()
                except Exception:
                    completed += 1
                    continue

                candidates[index] = enriched
                completed += 1

                if lead_update_cb:
                    try:
                        lead_update_cb(enriched)
                    except Exception:
                        pass

                progress_cb(
                    min(
                        97,
                        88 + int(
                            completed / max(1, len(work)) * 9
                        ),
                    ),
                    (
                        f"Bulk final enrichment "
                        f"{completed}/{len(work)}"
                    ),
                )
        finally:
            if self._user_stop:
                for future in futures:
                    if not future.done():
                        future.cancel()

            pool.shutdown(
                wait=False if self._user_stop else True,
                cancel_futures=True,
            )

        candidates.sort(
            key=lambda row: (
                self.quality_score(row),
                bool(row.get("google_maps_url")),
                bool(row.get("phone")),
                bool(row.get("email")),
                bool(row.get("website")),
            ),
            reverse=True,
        )

        return candidates[: self._target]

    def _round_limit(self, target, round_index, cached_count):
        target = max(1, int(target))
        cached_count = max(0, int(cached_count))

        # FAST mode must remain cache-aware without exploding the work.
        # Example target=30, cache=83:
        #   round 1 ~= 81 candidates (old behavior could exceed 140)
        #   round 2 ~= 111 candidates
        if getattr(self, "_search_mode", "Balanced") == "Fast":
            cache_allowance = min(
                target,
                math.ceil(cached_count * 0.25),
            )

            if round_index == 0:
                base = target * 2
            else:
                base = target * 3

            return max(
                target,
                min(180, int(base + cache_allowance)),
            )

        cache_pressure = min(4.0, cached_count / max(1, target))

        if round_index == 0:
            factor = 2.5 + min(1.5, cache_pressure)
        elif round_index == 1:
            factor = 4.0 + min(1.0, cache_pressure)
        else:
            factor = 6.0

        return max(
            30,
            min(
                500,
                int(target * factor) + 20,
            ),
        )

    def _source_round_limit(self, source, round_limit, cached_count):
        """
        Source-specific workload control.

        Bulk mode (151+) keeps Maps intentionally small and lets the faster
        DDGS-backed discovery sources build the candidate pool.
        """
        if self._bulk_mode:
            if source == "Google Maps":
                # About 18% of requested target, bounded so a 200+ lead job
                # does not turn into a very long browser crawl.
                maps_cap = max(
                    self._min_google_maps,
                    min(
                        55,
                        math.ceil(self._target * 0.18),
                    ),
                )
                return max(
                    self._min_google_maps,
                    min(int(round_limit), int(maps_cap)),
                )

            # Split the buffered candidate requirement across fast directory
            # sources, with overlap allowance for duplicates.
            fast_sources = [
                name
                for name in self._selected
                if name != "Google Maps"
            ]
            divisor = max(1, len(fast_sources))
            per_source = math.ceil(
                self._bulk_discovery_target
                / divisor
                * 1.55
            )
            return max(
                60,
                min(
                    180,
                    int(per_source),
                ),
            )

        if (
            getattr(self, "_search_mode", "Balanced") == "Fast"
            and source == "Google Maps"
        ):
            cache_allowance = min(
                math.ceil(self._target * 0.35),
                math.ceil(max(0, cached_count) * 0.12),
            )

            maps_cap = max(
                self._min_google_maps + 3,
                math.ceil(self._target * 0.50) + cache_allowance,
            )

            return max(
                self._min_google_maps,
                min(int(round_limit), int(maps_cap)),
            )

        return int(round_limit)

    @staticmethod
    def _split_sources(value):
        return [
            item.strip()
            for item in re.split(r"[,•|]", str(value or ""))
            if item.strip()
        ]

    @classmethod
    def _append_source(cls, lead, source_name):
        sources = cls._split_sources(
            lead.get("sources")
            or lead.get("source")
            or ""
        )

        if source_name and source_name not in sources:
            sources.append(source_name)

        lead["sources"] = ", ".join(sources)

    @staticmethod
    def _digits(value):
        return re.sub(r"\D+", "", str(value or ""))

    @classmethod
    def _maps_match_score(cls, base, candidate, city, state):
        """
        Conservative entity-resolution score.
        A Maps result must reach 60 points before it can enrich another source.
        """
        score = 0

        base_name = str(base.get("name") or "").strip().lower()
        candidate_name = str(candidate.get("name") or "").strip().lower()

        if base_name and candidate_name:
            ratio = SequenceMatcher(
                None,
                re.sub(r"[^a-z0-9]+", " ", base_name),
                re.sub(r"[^a-z0-9]+", " ", candidate_name),
            ).ratio()

            if ratio >= 0.90:
                score += 50
            elif ratio >= 0.78:
                score += 38
            elif ratio >= 0.65:
                score += 24

        base_phone = cls._digits(base.get("phone"))
        candidate_phone = cls._digits(candidate.get("phone"))

        if (
            base_phone
            and candidate_phone
            and base_phone[-10:] == candidate_phone[-10:]
        ):
            score += 40

        base_domain = cls._domain(base.get("website"))
        candidate_domain = cls._domain(candidate.get("website"))

        if base_domain and candidate_domain and base_domain == candidate_domain:
            score += 40

        candidate_location = " ".join([
            str(candidate.get("address") or ""),
            str(candidate.get("city") or ""),
            str(candidate.get("state") or ""),
        ]).lower()

        if city and str(city).strip().lower() in candidate_location:
            score += 20

        if (
            state
            and re.search(
                rf"\b{re.escape(str(state).strip())}\b",
                candidate_location,
                re.I,
            )
        ):
            score += 10

        return score

    @classmethod
    def _merge_maps_enrichment(cls, base, maps_row):
        merged = dict(base)

        # Existing non-empty values win. Maps fills gaps and provides its
        # specialist fields such as rating/reviews/coordinates/maps URL.
        for field in [
            "phone",
            "website",
            "email",
            "address",
            "rating",
            "reviews",
            "google_maps_url",
            "latitude",
            "longitude",
        ]:
            if not merged.get(field) and maps_row.get(field):
                merged[field] = maps_row[field]

        links = []
        for raw in (
            merged.get("source_links"),
            maps_row.get("source_links"),
            maps_row.get("google_maps_url"),
        ):
            if isinstance(raw, str):
                candidates = [
                    item.strip()
                    for item in raw.splitlines()
                    if item.strip()
                ]
            elif isinstance(raw, (list, tuple, set)):
                candidates = [
                    str(item).strip()
                    for item in raw
                    if str(item).strip()
                ]
            else:
                candidates = []

            for link in candidates:
                if link not in links:
                    links.append(link)

        merged["source_links"] = links
        cls._append_source(merged, "Google Maps")
        return merged

    @staticmethod
    def _needs_maps_enrichment(lead):
        if lead.get("google_maps_url"):
            return False

        missing = sum(
            1
            for field in ("phone", "address", "rating", "reviews")
            if not lead.get(field)
        )

        # Only spend a Maps lookup on leads that are materially incomplete.
        return missing >= 2

    def _cross_enrich_with_maps(
        self,
        results,
        country,
        city,
        state,
        zip_code,
        progress_cb,
        maps_adapter_ready,
    ):
        if self._user_stop:
            return results

        if "Google Maps" not in self._selected:
            return results

        if not maps_adapter_ready:
            return results

        adapter = self.adapters.get("Google Maps")
        if adapter is None:
            return results

        mode_caps = {
            "Fast": 2,
            "Balanced": 5,
            "Deep": 10,
        }
        cap = mode_caps.get(self._search_mode, 5)

        candidates = [
            lead
            for lead in results
            if self._needs_maps_enrichment(lead)
        ]

        # Enrich the most incomplete final leads first.
        candidates.sort(
            key=lambda lead: self.quality_score(lead)
        )
        candidates = candidates[:cap]

        if not candidates:
            return results

        progress_cb(
            96,
            f"Rich-detail pass: checking {len(candidates)} incomplete leads on Maps",
        )

        replacements = {}

        for index, lead in enumerate(candidates, start=1):
            if self._user_stop:
                break

            name = str(lead.get("name") or "").strip()
            if not name:
                continue

            try:
                rows = adapter.search(
                    keyword=name,
                    country=country,
                    city=city,
                    state=state,
                    zip_code=zip_code,
                    limit=3,
                    progress_cb=None,
                    lead_cb=None,
                ) or []
            except Exception:
                rows = []

            best = None
            best_score = -1

            for row in rows:
                score = self._maps_match_score(
                    lead,
                    row,
                    city,
                    state,
                )

                if score > best_score:
                    best = row
                    best_score = score

            # Require strong evidence before merging Maps information.
            if best is not None and best_score >= 60:
                replacements[id(lead)] = self._merge_maps_enrichment(
                    lead,
                    best,
                )

            progress_cb(
                min(99, 96 + int(index / max(1, len(candidates)) * 3)),
                (
                    f"Rich-detail pass {index}/{len(candidates)} • "
                    f"{name[:45]}"
                ),
            )

        if replacements:
            results = [
                replacements.get(id(lead), lead)
                for lead in results
            ]

        results.sort(
            key=lambda row: (
                self.quality_score(row),
                bool(row.get("google_maps_url")),
                bool(row.get("phone")),
                bool(row.get("email")),
                bool(row.get("website")),
            ),
            reverse=True,
        )

        return results

    def _fresh_count(self):
        return len(self._fresh_best_results(1000000))

    def search(
        self,
        keyword,
        country,
        city,
        state,
        zip_code,
        target_leads,
        sources,
        progress_cb,
        source_cb,
        lead_cb=None,
        lead_update_cb=None,
        search_mode="Balanced",
        skip_previous=True,
    ):
        self._target = max(1, int(target_leads))
        self._selected = [source for source in sources if source in self.adapters]
        self._search_mode = str(search_mode or "Balanced").strip().title()
        self._skip_previous = bool(skip_previous)

        # Automatic bulk optimization for large jobs.
        # The UI mode remains Fast/Balanced/Deep; this is an internal pipeline.
        self._bulk_mode = self._target >= 151
        self._bulk_discovery_complete = False
        self._bulk_discovery_target = self._bulk_target_size()

        self._scope_key = self.cache.scope_key(
            keyword,
            country,
            city,
            state,
            zip_code,
        )

        self._seen_keys = (
            self.cache.load_scope_keys(self._scope_key)
            if self._skip_previous
            else set()
        )

        self._stop_event.clear()
        self._pause_event.set()
        self._user_stop = False
        self._target_reached = False
        self._dedup = LeadDeduplicator()

        if "Google Maps" in self._selected:
            if self._bulk_mode:
                maps_share = 0.12
            else:
                maps_share = 0.20 if self._search_mode == "Fast" else 0.25

            self._min_google_maps = max(
                3,
                min(
                    self._target,
                    math.ceil(self._target * maps_share),
                ),
            )
        else:
            self._min_google_maps = 0

        with self._lock:
            self._source_counts = {source: 0 for source in self._selected}
            self._source_progress = {source: 0 for source in self._selected}
            self._source_done = set()

        if not self._selected:
            return [], {}, False

        mode_rounds = {"Fast": 2, "Balanced": 3, "Deep": 6}
        max_rounds = mode_rounds.get(self._search_mode, 3)
        keyword_rounds = self._keyword_rounds(keyword)[:max_rounds]
        cached_count = len(self._seen_keys)

        progress_cb(
            1,
            (
                f"Starting {len(self._selected)} sources • "
                f"Mode: {self._search_mode}"
                + (
                    f" + Bulk Discovery ({self._bulk_discovery_target} candidate goal)"
                    if self._bulk_mode
                    else ""
                )
                + f" • {len(self._seen_keys)} cached for this search"
            ),
        )

        enrichment_pool = ThreadPoolExecutor(
            max_workers=self.enrichment_workers,
            thread_name_prefix="lead-enrich",
        )

        enrichment_futures = set()
        scheduled_raw_keys = set()
        emitted_live_keys = set()

        # Fast mode intentionally limits expensive enrichment work.
        # Discovery can continue finding unique candidates, but we do not
        # deep-enrich every single one of them.
        fast_enrichment_budget = (
            max(self._target + 10, math.ceil(self._target * 1.5))
            if self._search_mode == "Fast"
            else None
        )
        enrichment_jobs_scheduled = 0

        def emit_unified(lead):
            if self._stop_event.is_set():
                return

            key = self.lead_key(lead)
            if not key or key in self._seen_keys:
                return

            unified, is_new, changed = self._dedup.add_or_merge(lead)
            unified_key = self.lead_key(unified)

            if unified_key in self._seen_keys:
                return

            if is_new:
                if lead_cb and unified_key not in emitted_live_keys:
                    emitted_live_keys.add(unified_key)
                    lead_cb(unified)
            elif changed and lead_update_cb:
                lead_update_cb(unified)

            total = len(self._fresh_best_results(1000000))
            qualified = self._qualified_count()

            progress_cb(
                self._overall_progress(),
                f"{total} new candidates • {qualified}/{self._target} qualified",
            )

            if self._bulk_mode:
                if total >= self._bulk_discovery_target:
                    self._bulk_discovery_complete = True
                    self._cancel_discovery_adapters()
                return

            if self._can_finish_target():
                self._target_reached = True
                self.stop(user_requested=False)

        def enrich_and_emit(lead):
            if self._stop_event.is_set():
                return

            self._wait_if_paused()
            if self._stop_event.is_set():
                return

            try:
                enriched = self.enricher.enrich(
                    lead,
                    mode=self._search_mode,
                )
            except Exception:
                enriched = dict(lead)

            emit_unified(enriched)

        def run_source(source):
            adapter = self.adapters[source]

            def source_progress(value, message):
                if self._stop_event.is_set():
                    return

                with self._lock:
                    self._source_progress[source] = max(0, min(100, int(value)))

                progress_cb(
                    self._overall_progress(),
                    f"{source}: {message}",
                )

            def source_lead(raw_lead):
                if self._stop_event.is_set():
                    return

                self._wait_if_paused()
                if self._stop_event.is_set():
                    return

                lead = dict(raw_lead or {})
                lead.setdefault("source", source)
                lead.setdefault("sources", source)
                lead["keyword"] = keyword
                lead.setdefault("country", country or "USA")
                lead.setdefault("city", city)
                lead.setdefault("state", state)
                lead.setdefault("zip_code", zip_code)

                raw_key = self.lead_key(lead)
                if raw_key and raw_key in self._seen_keys:
                    return

                with self._lock:
                    self._source_counts[source] = self._source_counts.get(source, 0) + 1
                    source_count = self._source_counts[source]

                source_cb(source, source_count)

                if source == "Google Maps":
                    emit_unified(lead)
                    return

                # BULK MODE: discovery first. Do not enrich every candidate
                # while sources are still running. Raw location-safe candidates
                # enter dedup immediately; only the final selected set is
                # enriched later.
                if self._bulk_mode:
                    emit_unified(lead)
                    return

                # In Fast mode, already-useful leads should appear immediately
                # instead of being delayed by unnecessary HTTP requests.
                if (
                    self._search_mode == "Fast"
                    and lead.get("phone")
                    and lead.get("website")
                ):
                    emit_unified(lead)
                    return

                # Prevent duplicate enrichment jobs and enforce a Fast-mode
                # enrichment budget. Extra discovered leads are still kept as
                # raw candidates, so uniqueness is not lost.
                nonlocal enrichment_jobs_scheduled

                with self._lock:
                    if raw_key and raw_key in scheduled_raw_keys:
                        return

                    if (
                        fast_enrichment_budget is not None
                        and enrichment_jobs_scheduled >= fast_enrichment_budget
                    ):
                        schedule_enrichment = False
                    else:
                        schedule_enrichment = True
                        enrichment_jobs_scheduled += 1
                        if raw_key:
                            scheduled_raw_keys.add(raw_key)

                if not schedule_enrichment:
                    emit_unified(lead)
                    return

                future = enrichment_pool.submit(enrich_and_emit, lead)
                enrichment_futures.add(future)

            try:
                for round_index, round_keyword in enumerate(keyword_rounds):
                    # Fast mode has a hard two-round budget. Never add hidden
                    # extra depth just to force the requested target.
                    if self._search_mode == "Fast" and round_index >= 2:
                        break

                    if self._stop_event.is_set():
                        break

                    if self._bulk_mode and self._bulk_discovery_complete:
                        break

                    self._wait_if_paused()
                    if self._stop_event.is_set():
                        break

                    if (
                        not self._bulk_mode
                        and self._can_finish_target()
                    ):
                        self._target_reached = True
                        self.stop(user_requested=False)
                        break

                    round_limit = self._round_limit(
                        self._target,
                        round_index,
                        cached_count,
                    )
                    source_limit = self._source_round_limit(
                        source,
                        round_limit,
                        cached_count,
                    )

                    source_progress(
                        min(92, round_index * 15),
                        (
                            f"depth {round_index + 1}/{len(keyword_rounds)} • "
                            f"{round_keyword} • limit {source_limit}"
                        ),
                    )

                    rows = adapter.search(
                        keyword=round_keyword,
                        country=country,
                        city=city,
                        state=state,
                        zip_code=zip_code,
                        limit=source_limit,
                        progress_cb=source_progress,
                        lead_cb=source_lead,
                    ) or []

                    for row in rows:
                        if self._stop_event.is_set():
                            break

                        item = dict(row)
                        item["keyword"] = keyword
                        source_lead(item)

                    fresh_now = self._fresh_count()
                    qualified_now = self._qualified_count()

                    progress_cb(
                        self._overall_progress(),
                        (
                            f"{fresh_now}/{self._target} fresh • "
                            f"{qualified_now} qualified • "
                            f"depth {round_index + 1}"
                        ),
                    )

                    if self._bulk_mode:
                        if fresh_now >= self._bulk_discovery_target:
                            self._bulk_discovery_complete = True
                            self._cancel_discovery_adapters()
                            break
                    elif fresh_now >= self._target and self._can_finish_target():
                        self._target_reached = True
                        self.stop(user_requested=False)
                        break

            except Exception as exc:
                if not self._stop_event.is_set():
                    progress_cb(
                        self._overall_progress(),
                        f"{source} error: {exc}",
                    )

            finally:
                with self._lock:
                    self._source_progress[source] = 100
                    self._source_done.add(source)

            return source

        worker_count = min(self.max_workers, len(self._selected))

        source_executor = ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="lead-source",
        )
        futures = {
            source_executor.submit(run_source, source): source
            for source in self._selected
        }
        all_source_futures = dict(futures)

        try:
            while futures:
                done, _ = wait(
                    futures,
                    timeout=0.15,
                    return_when=FIRST_COMPLETED,
                )

                for future in done:
                    futures.pop(future, None)

                if (
                    self._target_reached
                    or self._user_stop
                    or (self._bulk_mode and self._bulk_discovery_complete)
                ):
                    # Bulk completion cancels discovery only; final enrichment
                    # still runs. Normal target/user stop uses the full stop path.
                    if self._bulk_mode and self._bulk_discovery_complete and not self._user_stop:
                        self._cancel_discovery_adapters()
                    else:
                        self.stop(user_requested=self._user_stop)

                    for future in futures:
                        future.cancel()
                    break

                self._wait_if_paused()
        finally:
            # IMPORTANT: ThreadPoolExecutor's context manager waits by default.
            # We explicitly use wait=False so a reached target returns promptly.
            source_executor.shutdown(
                wait=False,
                cancel_futures=True,
            )

        # Only wait for enrichment while the target has NOT been reached.
        # The moment enough qualified unique leads exist, cancel queued jobs.
        if not self._stop_event.is_set() and not self._bulk_mode:
            for future in as_completed(list(enrichment_futures)):
                if self._target_reached or self._user_stop:
                    break

                try:
                    future.result()
                except Exception:
                    pass

                if self._can_finish_target():
                    self._target_reached = True
                    self.stop(user_requested=False)
                    break

        if self._target_reached or self._user_stop:
            for future in enrichment_futures:
                future.cancel()

        enrichment_pool.shutdown(
            wait=False,
            cancel_futures=True,
        )

        results = self._fresh_best_results(self._target)

        if self._bulk_mode and not self._user_stop:
            progress_cb(
                86,
                (
                    f"Bulk discovery complete — "
                    f"{self._fresh_count()} unique candidates; "
                    f"enriching selected {min(self._target, len(results))}"
                ),
            )

            results = self._bulk_final_enrich(
                results,
                progress_cb=progress_cb,
                lead_update_cb=lead_update_cb,
            )

            # The requested count has now been selected/finalized.
            if len(results) >= self._target:
                self._target_reached = True

        # Rich-detail pass for incomplete non-Maps final leads.
        # We never overlap this with an active Google Maps source worker.
        maps_future = None
        for future, source_name in all_source_futures.items():
            if source_name == "Google Maps":
                maps_future = future
                break

        maps_adapter_ready = (
            maps_future is None
            or maps_future.done()
        )

        # If target cancellation just stopped Maps, give it a very short grace
        # period to release its subprocess. If it is still busy, skip the
        # cross-enrichment rather than creating competing browser jobs.
        if (
            maps_future is not None
            and not maps_adapter_ready
            and not self._user_stop
        ):
            try:
                maps_future.result(timeout=2.0)
                maps_adapter_ready = True
            except Exception:
                maps_adapter_ready = maps_future.done()

        if not self._user_stop and not self._bulk_mode:
            results = self._cross_enrich_with_maps(
                results=results,
                country=country,
                city=city,
                state=state,
                zip_code=zip_code,
                progress_cb=progress_cb,
                maps_adapter_ready=maps_adapter_ready,
            )

        # Cache only leads actually returned in this completed/stopped result set.
        cache_rows = []
        for row in results:
            key = self.lead_key(row)
            if key:
                cache_rows.append((row, key))
                self._seen_keys.add(key)

        self.cache.add_many(self._scope_key, cache_rows)

        with self._lock:
            counts = dict(self._source_counts)

        qualified = sum(1 for row in results if self.is_qualified(row))

        if self._user_stop:
            progress_cb(
                100,
                f"Stopped — kept {len(results)} new leads ({qualified} qualified)",
            )
            return results, counts, True

        if self._target_reached:
            if self._bulk_mode:
                progress_cb(
                    100,
                    (
                        f"Bulk complete — {len(results)} new unique leads • "
                        f"discovery-first pipeline"
                    ),
                )
            else:
                progress_cb(
                    100,
                    f"Target reached — {len(results)} new unique leads",
                )
            return results, counts, False

        if self._bulk_mode and len(results) < self._target:
            progress_cb(
                100,
                (
                    f"Bulk pool exhausted — {len(results)} new unique leads "
                    f"found (target {self._target})"
                ),
            )
            return results, counts, False

        if self._search_mode == "Fast" and len(results) < self._target:
            progress_cb(
                100,
                (
                    f"Fresh pool exhausted — {len(results)} new unique leads "
                    f"found within the Fast-mode search budget"
                ),
            )
            return results, counts, False

        progress_cb(
            100,
            f"Complete — {len(results)} new unique leads ({qualified} qualified)",
        )
        return results, counts, False
