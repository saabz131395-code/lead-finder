from google_maps_source import (
    search_google_maps,
    cancel_google_maps,
    pause_google_maps,
    resume_google_maps,
)


class GoogleMapsAdapter:
    def __init__(self):
        self.cancel_requested = False

    def cancel(self):
        self.cancel_requested = True
        cancel_google_maps()

    def pause(self):
        pause_google_maps()

    def resume(self):
        resume_google_maps()

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

        results = search_google_maps(
            keyword=keyword,
            city=city,
            state=state,
            country=country or "USA",
            limit=limit,
            progress_cb=progress_cb,
            lead_cb=lead_cb,
            cancel_event=lambda: self.cancel_requested,
        )

        cleaned = []

        for row in results:
            item = dict(row)
            item["source"] = "Google Maps"
            item["sources"] = "Google Maps"
            item.setdefault("zip_code", zip_code)
            cleaned.append(item)

        return cleaned[:limit]
