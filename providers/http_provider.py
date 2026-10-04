from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

try:
    from scrapling.fetchers import Fetcher
except Exception:
    Fetcher = None


class HTTPProvider:
    """
    Fast hybrid fetch provider.

    Order:
      1. Scrapling Fetcher (primary)
      2. httpx persistent client (fallback)

    This keeps normal pages fast while preserving a lightweight fallback.
    Browser automation is intentionally NOT used here.
    """

    def __init__(self, timeout=7.0):
        self.timeout = float(timeout)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        self.client = httpx.Client(
            headers=self.headers,
            timeout=self.timeout,
            follow_redirects=True,
            http2=True,
        )

    def fetch(self, url):
        url = str(url or "").strip()
        if not url:
            return None

        # 1) Scrapling first: benchmarked faster for this project.
        scrapling_result = self._fetch_scrapling(url)
        if scrapling_result:
            return scrapling_result

        # 2) Lightweight direct HTTP fallback.
        return self._fetch_httpx(url)

    def _fetch_scrapling(self, url):
        if Fetcher is None:
            return None

        try:
            page = Fetcher.get(url)
            if page is None:
                return None

            status = self._status_code(page)
            if status is not None and int(status) >= 400:
                return None

            html = self._page_html(page)
            if not html or len(html) < 80:
                return None

            final_url = self._page_url(page) or url

            return {
                "url": final_url,
                "text": html,
                "status": int(status) if status is not None else 200,
                "provider": "scrapling",
            }
        except Exception:
            return None

    def _fetch_httpx(self, url):
        try:
            response = self.client.get(url)

            if response.status_code >= 400:
                return None

            ctype = response.headers.get("content-type", "").lower()
            if ctype and "html" not in ctype and "text" not in ctype:
                return None

            text = response.text or ""
            if len(text) < 80:
                return None

            return {
                "url": str(response.url),
                "text": text,
                "status": response.status_code,
                "provider": "httpx",
            }
        except Exception:
            return None

    @staticmethod
    def _status_code(page):
        for attr in ("status", "status_code", "statuscode"):
            value = getattr(page, attr, None)
            if value is not None and not callable(value):
                try:
                    return int(value)
                except Exception:
                    pass

        response = getattr(page, "response", None)
        if response is not None:
            for attr in ("status", "status_code"):
                value = getattr(response, attr, None)
                if value is not None:
                    try:
                        return int(value)
                    except Exception:
                        pass
        return None

    @staticmethod
    def _page_html(page):
        for attr in ("html_content", "html", "text", "body"):
            value = getattr(page, attr, None)
            if value:
                try:
                    if isinstance(value, bytes):
                        return value.decode("utf-8", errors="ignore")
                    return str(value)
                except Exception:
                    pass

        try:
            value = str(page)
            if "<" in value and ">" in value:
                return value
        except Exception:
            pass
        return ""

    @staticmethod
    def _page_url(page):
        for attr in ("url", "final_url"):
            value = getattr(page, attr, None)
            if value:
                return str(value)
        return ""

    @staticmethod
    def soup(html):
        try:
            return BeautifulSoup(html or "", "html.parser")
        except Exception:
            return None

    @staticmethod
    def absolute(base_url, href):
        try:
            return urljoin(base_url, href)
        except Exception:
            return ""

    @staticmethod
    def domain(url):
        try:
            return urlparse(url).netloc.lower().replace("www.", "")
        except Exception:
            return ""

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass
