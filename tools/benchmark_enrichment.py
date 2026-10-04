import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from bs4 import BeautifulSoup

from scrapling.fetchers import Fetcher


URLS = [
    "https://www.reliantplumbing.com/",
    "https://www.clarkekentplumbing.com/",
    "https://www.abacusplumbing.net/",
    "https://www.rooter-man.com/",
    "https://www.mrrooter.com/",
    "https://www.ars.com/",
    "https://www.rotorooter.com/",
    "https://www.benjaminfranklinplumbing.com/",
    "https://www.oncallplumbing.com/",
    "https://www.austintechnicalplumbing.com/",
]


EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)

PHONE_RE = re.compile(
    r"(?:\+?1[\s.\-]?)?(?:\(\d{3}\)|\d{3})[\s.\-]\d{3}[\s.\-]\d{4}"
)


def extract_fields(html):
    html = html or ""
    soup = BeautifulSoup(html, "html.parser")
    text = " ".join(soup.stripped_strings)

    emails = set(EMAIL_RE.findall(text))
    phones = set(PHONE_RE.findall(text))

    social_links = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()

        if any(
            domain in href.lower()
            for domain in [
                "facebook.com",
                "instagram.com",
                "linkedin.com",
                "twitter.com",
                "x.com",
            ]
        ):
            social_links.add(href)

    return {
        "emails": emails,
        "phones": phones,
        "socials": social_links,
    }


def benchmark_httpx(url):
    started = time.perf_counter()

    try:
        with httpx.Client(
            timeout=10,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/124 Safari/537.36"
                )
            },
        ) as client:
            response = client.get(url)

        elapsed = time.perf_counter() - started

        if response.status_code >= 400:
            return {
                "url": url,
                "success": False,
                "time": elapsed,
                "emails": 0,
                "phones": 0,
                "socials": 0,
            }

        fields = extract_fields(response.text)

        return {
            "url": url,
            "success": True,
            "time": elapsed,
            "emails": len(fields["emails"]),
            "phones": len(fields["phones"]),
            "socials": len(fields["socials"]),
        }

    except Exception:
        return {
            "url": url,
            "success": False,
            "time": time.perf_counter() - started,
            "emails": 0,
            "phones": 0,
            "socials": 0,
        }


def benchmark_scrapling(url):
    started = time.perf_counter()

    try:
        page = Fetcher.get(url)

        elapsed = time.perf_counter() - started

        html = getattr(page, "html_content", None)

        if not html:
            try:
                html = page.html
            except Exception:
                html = str(page)

        fields = extract_fields(str(html))

        return {
            "url": url,
            "success": True,
            "time": elapsed,
            "emails": len(fields["emails"]),
            "phones": len(fields["phones"]),
            "socials": len(fields["socials"]),
        }

    except Exception:
        return {
            "url": url,
            "success": False,
            "time": time.perf_counter() - started,
            "emails": 0,
            "phones": 0,
            "socials": 0,
        }


def run_parallel(name, function):
    print()
    print("=" * 70)
    print(name)
    print("=" * 70)

    started = time.perf_counter()

    results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(function, url): url
            for url in URLS
        }

        for future in as_completed(futures):
            result = future.result()
            results.append(result)

            status = "OK" if result["success"] else "FAIL"

            print(
                f"{status:4} | "
                f"{result['time']:6.2f}s | "
                f"E:{result['emails']:2} "
                f"P:{result['phones']:2} "
                f"S:{result['socials']:2} | "
                f"{result['url']}"
            )

    total_time = time.perf_counter() - started

    successes = sum(
        1
        for result in results
        if result["success"]
    )

    emails = sum(
        result["emails"]
        for result in results
    )

    phones = sum(
        result["phones"]
        for result in results
    )

    socials = sum(
        result["socials"]
        for result in results
    )

    average = (
        sum(result["time"] for result in results)
        / len(results)
        if results
        else 0
    )

    print()
    print(f"Successful pages : {successes}/{len(URLS)}")
    print(f"Total wall time  : {total_time:.2f}s")
    print(f"Avg page time    : {average:.2f}s")
    print(f"Emails found     : {emails}")
    print(f"Phones found     : {phones}")
    print(f"Social links     : {socials}")

    return {
        "successes": successes,
        "wall_time": total_time,
        "average": average,
        "emails": emails,
        "phones": phones,
        "socials": socials,
    }


if __name__ == "__main__":
    print("Starting enrichment benchmark...")
    print(f"Testing {len(URLS)} URLs with 5 parallel workers.")

    httpx_result = run_parallel(
        "HTTPX + BeautifulSoup",
        benchmark_httpx,
    )

    scrapling_result = run_parallel(
        "Scrapling Fetcher + BeautifulSoup extraction",
        benchmark_scrapling,
    )

    print()
    print("=" * 70)
    print("FINAL COMPARISON")
    print("=" * 70)

    print(
        f"HTTPX     : "
        f"{httpx_result['wall_time']:.2f}s | "
        f"{httpx_result['successes']} success | "
        f"{httpx_result['emails']} emails | "
        f"{httpx_result['phones']} phones"
    )

    print(
        f"Scrapling : "
        f"{scrapling_result['wall_time']:.2f}s | "
        f"{scrapling_result['successes']} success | "
        f"{scrapling_result['emails']} emails | "
        f"{scrapling_result['phones']} phones"
    )

    if scrapling_result["wall_time"] < httpx_result["wall_time"]:
        print()
        print("RESULT: Scrapling was faster.")
    else:
        print()
        print("RESULT: HTTPX was faster.")

    print()
    print(
        "Important: we will choose based on BOTH speed and data quality, "
        "not speed alone."
    )