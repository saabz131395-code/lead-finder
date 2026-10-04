import re
import threading
from copy import deepcopy
from urllib.parse import urlparse


class LeadDeduplicator:
    """
    Thread-safe lead store.

    add_or_merge() returns:
        (lead_snapshot, is_new, changed)
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._items = []
        self._keys = {}

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
            host = urlparse(value).netloc.lower().replace("www.", "")
            return host
        except Exception:
            return ""

    @staticmethod
    def _phone(value):
        return re.sub(r"\D+", "", str(value or ""))

    def _candidate_keys(self, lead):
        keys = []

        maps_url = str(lead.get("google_maps_url") or "").strip()
        if maps_url:
            keys.append("maps:" + maps_url)

        phone = self._phone(lead.get("phone"))
        if phone:
            keys.append("phone:" + phone)

        domain = self._domain(lead.get("website"))
        if domain:
            keys.append("domain:" + domain)

        name = self._norm(lead.get("name"))
        address = self._norm(lead.get("address"))
        city = self._norm(lead.get("city"))

        if name and address:
            keys.append(f"name-address:{name}|{address}")
        if name and city:
            keys.append(f"name-city:{name}|{city}")

        return keys

    def _find_index(self, lead):
        for key in self._candidate_keys(lead):
            if key in self._keys:
                return self._keys[key]

        # Conservative fallback comparison.
        name = self._norm(lead.get("name"))
        address = self._norm(lead.get("address"))
        city = self._norm(lead.get("city"))

        if not name:
            return None

        for index, item in enumerate(self._items):
            if self._norm(item.get("name")) != name:
                continue

            item_address = self._norm(item.get("address"))
            item_city = self._norm(item.get("city"))

            if address and item_address and address == item_address:
                return index
            if city and item_city and city == item_city:
                return index

        return None

    def _register_keys(self, index, lead):
        for key in self._candidate_keys(lead):
            self._keys[key] = index

    @staticmethod
    def _as_links(value):
        if not value:
            return []
        if isinstance(value, (list, tuple, set)):
            return [str(x).strip() for x in value if str(x).strip()]
        return [x.strip() for x in str(value).splitlines() if x.strip()]

    @staticmethod
    def _source_list(value):
        if not value:
            return []
        return [x.strip() for x in str(value).split(",") if x.strip()]

    def _merge_into(self, target, incoming):
        changed = False

        richer_fields = [
            "keyword", "city", "state", "country", "zip_code",
            "phone", "website", "email", "address",
            "rating", "reviews", "linkedin", "facebook", "instagram",
            "google_maps_url", "latitude", "longitude",
        ]

        for field in richer_fields:
            old = target.get(field)
            new = incoming.get(field)
            if not old and new:
                target[field] = new
                changed = True

        existing_sources = self._source_list(
            target.get("sources") or target.get("source")
        )
        incoming_sources = self._source_list(
            incoming.get("sources") or incoming.get("source")
        )

        for source in incoming_sources:
            if source not in existing_sources:
                existing_sources.append(source)
                changed = True

        if existing_sources:
            target["sources"] = ", ".join(existing_sources)

        links = self._as_links(target.get("source_links"))
        for link in self._as_links(incoming.get("source_links")):
            if link not in links:
                links.append(link)
                changed = True
        target["source_links"] = links

        old_notes = str(target.get("notes") or "").strip()
        new_notes = str(incoming.get("notes") or "").strip()
        if new_notes and new_notes not in old_notes:
            target["notes"] = (
                old_notes + "\n" + new_notes
                if old_notes
                else new_notes
            )
            changed = True

        return changed

    def add_or_merge(self, lead):
        incoming = dict(lead or {})

        with self._lock:
            index = self._find_index(incoming)

            if index is None:
                incoming.setdefault("source", "")
                incoming.setdefault("sources", incoming.get("source", ""))
                incoming.setdefault("source_links", [])
                incoming.setdefault("notes", "")
                incoming.setdefault("zip_code", "")
                incoming.setdefault("google_maps_url", "")
                incoming.setdefault("latitude", "")
                incoming.setdefault("longitude", "")

                self._items.append(incoming)
                index = len(self._items) - 1
                self._register_keys(index, incoming)
                return deepcopy(incoming), True, True

            target = self._items[index]
            changed = self._merge_into(target, incoming)
            self._register_keys(index, target)

            return deepcopy(target), False, changed

    def snapshot(self, limit=None):
        with self._lock:
            rows = deepcopy(self._items)

        if limit is not None:
            rows = rows[: int(limit)]
        return rows

    def count(self):
        with self._lock:
            return len(self._items)
