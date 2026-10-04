from scrapling.fetchers import Fetcher

url = "https://www.nycplumber.net/"

page = Fetcher.get(url)

links = page.css("a::attr(href)").getall()

socials = [
    link for link in links
    if any(site in link.lower() for site in [
        "linkedin.com",
        "facebook.com",
        "instagram.com",
        "twitter.com",
        "x.com"
    ])
]

print("Status:", page.status)
print("Social links:", socials)