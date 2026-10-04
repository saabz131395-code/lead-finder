from adapters import GoogleMapsAdapter, DDGSSourceAdapter
from engine.hybrid_engine import HybridSourceEngine


class SourceManager:
    def __init__(self):
        self.adapters = {
            "Google Maps": GoogleMapsAdapter(),
            "Hotfrog": DDGSSourceAdapter("Hotfrog", ["hotfrog.com"]),
            "Yellow Pages": DDGSSourceAdapter("Yellow Pages", ["yellowpages.com"]),
            "LinkedIn": DDGSSourceAdapter(
                "LinkedIn",
                ["linkedin.com/company", "linkedin.com/in"],
            ),
        }

        self.engine = HybridSourceEngine(
            adapters=self.adapters,
            max_workers=6,
            enrichment_workers=8,
            cache_max_rows=20000,
        )

    def cancel(self):
        self.engine.stop(user_requested=True)

    def pause(self):
        self.engine.pause()

    def resume(self):
        self.engine.resume()

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
        search_mode="Balanced",
        skip_previous=True,
    ):
        return self.engine.search(
            keyword=keyword,
            country=country,
            city=city,
            state=state,
            zip_code=zip_code,
            target_leads=target_leads,
            sources=sources,
            progress_cb=progress_cb,
            source_cb=source_cb,
            lead_cb=lead_cb,
            search_mode=search_mode,
            skip_previous=skip_previous,
        )
