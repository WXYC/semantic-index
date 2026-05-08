"""
Download and extract music reviews from the Wayback Machine and live sites.

Usage:
    python scripts/crawl_reviews.py tmt       # Tiny Mix Tapes (from Wayback)
    python scripts/crawl_reviews.py quietus   # The Quietus (live)
    python scripts/crawl_reviews.py bandcamp  # Bandcamp Daily (live)
    python scripts/crawl_reviews.py stereogum # Stereogum (live)

Reviews are saved as JSONL files in data/reviews/<source>/reviews.jsonl.
Each line: {"url": "...", "artist": "...", "album": "...", "title": "...",
            "author": "...", "date": "...", "text": "...", "source": "..."}

Resumable: skips URLs already in the output file.
"""

import argparse
import json
import logging
import re
import time
from pathlib import Path

import requests
import trafilatura

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
)

DATA_DIR = Path("data/reviews")


def load_done(output_path: Path) -> set[str]:
    """Load URLs already downloaded from the JSONL output file."""
    done = set()
    if output_path.exists():
        with open(output_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        done.add(obj.get("url", ""))
                    except json.JSONDecodeError:
                        pass
    return done


def fetch_wayback(url: str, timestamp: str) -> str | None:
    """Fetch a page from the Wayback Machine."""
    wb_url = f"http://web.archive.org/web/{timestamp}id_/{url}"
    try:
        resp = SESSION.get(wb_url, timeout=30)
        if resp.status_code == 200:
            return resp.text
    except requests.RequestException as e:
        log.warning("Failed to fetch %s: %s", url, e)
    return None


def fetch_live(url: str) -> str | None:
    """Fetch a live page."""
    try:
        resp = SESSION.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.text
    except requests.RequestException as e:
        log.warning("Failed to fetch %s: %s", url, e)
    return None


def extract_text(html: str, url: str) -> dict | None:
    """Extract article text and metadata using trafilatura."""
    result = trafilatura.extract(
        html,
        url=url,
        output_format="json",
        include_comments=False,
        include_tables=False,
        favor_precision=True,
        with_metadata=True,
    )
    if result:
        return json.loads(result)
    return None


# --- TMT (Tiny Mix Tapes via Wayback Machine) ---


def crawl_tmt():
    """Download TMT reviews from the Wayback Machine."""
    url_file = DATA_DIR / "tmt" / "urls.txt"
    output = DATA_DIR / "tmt" / "reviews.jsonl"
    done = load_done(output)

    with open(url_file) as f:
        entries = []
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                entries.append((parts[0], parts[1]))

    log.info("TMT: %d URLs to process, %d already done", len(entries), len(done))

    # Filter out listing pages and non-review URLs
    entries = [
        (url, ts)
        for url, ts in entries
        if "/music-review/" in url
        and "?" not in url
        and url.rstrip("/").count("/") >= 4  # has a slug after /music-review/
    ]
    log.info("TMT: %d review URLs after filtering", len(entries))

    success = 0
    errors = 0
    skipped = len(done)

    with open(output, "a") as out:
        for url, timestamp in entries:
            if url in done:
                continue

            html = fetch_wayback(url, timestamp)
            if not html:
                errors += 1
                time.sleep(0.5)
                continue

            extracted = extract_text(html, url)
            if extracted and extracted.get("text"):
                record = {
                    "url": url,
                    "title": extracted.get("title", ""),
                    "author": extracted.get("author", ""),
                    "date": extracted.get("date", ""),
                    "text": extracted["text"],
                    "source": "tinymixtapes",
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()
                success += 1
                done.add(url)
            else:
                errors += 1

            if (success + errors) % 50 == 0:
                total = success + errors + skipped
                log.info(
                    "TMT progress: %d/%d (%.0f%%) | %d extracted, %d errors",
                    total,
                    len(entries),
                    total / len(entries) * 100,
                    success,
                    errors,
                )

            # Polite rate: ~1 req/sec for Wayback
            time.sleep(1.0)

    log.info("TMT done: %d extracted, %d errors", success, errors)


# --- The Quietus ---


def discover_quietus_urls() -> list[str]:
    """Enumerate Quietus article URLs from WordPress sitemaps.

    The Quietus uses /quietus-reviews/ (not /reviews/) for reviews,
    plus /interviews/, /culture/, /opinion-and-essays/ for features.
    The listing pages are JS-rendered so we use sitemaps instead.
    """
    resp = SESSION.get("https://thequietus.com/sitemap_index.xml", timeout=30)
    if resp.status_code != 200:
        log.error("Failed to fetch Quietus sitemap index: %d", resp.status_code)
        return []

    sitemaps = re.findall(r"<loc>([^<]+)</loc>", resp.text)
    post_sitemaps = [s for s in sitemaps if "post-sitemap" in s]
    log.info("Quietus: found %d post sitemaps", len(post_sitemaps))

    all_urls = []
    for sm_url in post_sitemaps:
        resp = SESSION.get(sm_url, timeout=30)
        if resp.status_code == 200:
            urls = re.findall(r"<loc>([^<]+)</loc>", resp.text)
            all_urls.extend(urls)
        time.sleep(0.3)

    # Keep reviews, interviews, essays, and culture pieces
    article_prefixes = ("/quietus-reviews/", "/interviews/", "/culture/", "/opinion-and-essays/")
    articles = [u for u in all_urls if any(p in u for p in article_prefixes)]
    log.info(
        "Quietus: %d articles from sitemaps (%d reviews, %d interviews, %d culture, %d essays)",
        len(articles),
        sum(1 for u in articles if "/quietus-reviews/" in u),
        sum(1 for u in articles if "/interviews/" in u),
        sum(1 for u in articles if "/culture/" in u),
        sum(1 for u in articles if "/opinion-and-essays/" in u),
    )
    return articles


def crawl_quietus():
    """Download Quietus reviews."""
    output = DATA_DIR / "quietus" / "reviews.jsonl"
    url_cache = DATA_DIR / "quietus" / "urls.txt"
    done = load_done(output)

    # Discover or load cached URLs
    if url_cache.exists():
        with open(url_cache) as f:
            urls = [line.strip() for line in f if line.strip()]
        log.info("Quietus: loaded %d cached URLs", len(urls))
    else:
        log.info("Quietus: discovering review URLs...")
        urls = discover_quietus_urls()
        with open(url_cache, "w") as f:
            f.write("\n".join(urls))
        log.info("Quietus: found %d review URLs", len(urls))

    log.info("Quietus: %d to process, %d already done", len(urls), len(done))

    success = 0
    errors = 0

    with open(output, "a") as out:
        for i, url in enumerate(urls):
            if url in done:
                continue

            html = fetch_live(url)
            if not html:
                errors += 1
                time.sleep(2.0)
                continue

            extracted = extract_text(html, url)
            if extracted and extracted.get("text"):
                record = {
                    "url": url,
                    "title": extracted.get("title", ""),
                    "author": extracted.get("author", ""),
                    "date": extracted.get("date", ""),
                    "text": extracted["text"],
                    "source": "thequietus",
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()
                success += 1
                done.add(url)
            else:
                errors += 1

            if (success + errors) % 50 == 0:
                log.info(
                    "Quietus progress: %d/%d | %d extracted, %d errors",
                    i + 1,
                    len(urls),
                    success,
                    errors,
                )

            time.sleep(2.0)

    log.info("Quietus done: %d extracted, %d errors", success, errors)


# --- Bandcamp Daily ---


def discover_bandcamp_daily_urls() -> list[str]:
    """Enumerate Bandcamp Daily article URLs via Wayback Machine CDX API."""
    sections = [
        "album-of-the-day",
        "features",
        "lists",
        "best-of",
        "big-ups",
        "label-profile",
        "scene-report",
        "lifetime-achievement",
        "seven-essential-releases",
    ]

    all_urls = set()
    for section in sections:
        resp = SESSION.get(
            "http://web.archive.org/cdx/search/cdx",
            params={
                "url": f"daily.bandcamp.com/{section}/*",
                "collapse": "urlkey",
                "filter": "statuscode:200",
                "fl": "original",
                "output": "json",
            },
            timeout=120,
        )
        if resp.status_code == 200:
            rows = resp.json()
            for r in rows[1:]:
                u = r[0].replace("http://", "https://").split("?")[0].rstrip("/")
                if u.count("/") >= 4:
                    all_urls.add(u)
            log.info("Bandcamp Daily CDX %s: %d URLs", section, len(all_urls))

    log.info("Bandcamp Daily: %d total unique article URLs from CDX", len(all_urls))
    return sorted(all_urls)


def crawl_bandcamp_daily():
    """Download Bandcamp Daily articles."""
    output = DATA_DIR / "bandcamp-daily" / "reviews.jsonl"
    url_cache = DATA_DIR / "bandcamp-daily" / "urls.txt"
    done = load_done(output)

    if url_cache.exists():
        with open(url_cache) as f:
            urls = [line.strip() for line in f if line.strip()]
        log.info("Bandcamp Daily: loaded %d cached URLs", len(urls))
    else:
        log.info("Bandcamp Daily: discovering article URLs...")
        urls = discover_bandcamp_daily_urls()
        with open(url_cache, "w") as f:
            f.write("\n".join(urls))
        log.info("Bandcamp Daily: found %d article URLs", len(urls))

    log.info("Bandcamp Daily: %d to process, %d already done", len(urls), len(done))

    success = 0
    errors = 0

    with open(output, "a") as out:
        for i, url in enumerate(urls):
            if url in done:
                continue

            html = fetch_live(url)
            if not html:
                errors += 1
                time.sleep(2.0)
                continue

            extracted = extract_text(html, url)
            if extracted and extracted.get("text"):
                record = {
                    "url": url,
                    "title": extracted.get("title", ""),
                    "author": extracted.get("author", ""),
                    "date": extracted.get("date", ""),
                    "text": extracted["text"],
                    "source": "bandcamp_daily",
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()
                success += 1
                done.add(url)
            else:
                errors += 1

            if (success + errors) % 50 == 0:
                log.info(
                    "Bandcamp Daily progress: %d/%d | %d extracted, %d errors",
                    i + 1,
                    len(urls),
                    success,
                    errors,
                )

            time.sleep(2.0)

    log.info("Bandcamp Daily done: %d extracted, %d errors", success, errors)


# --- Stereogum ---


def discover_stereogum_urls() -> list[str]:
    """Enumerate Stereogum article URLs from Next.js __NEXT_DATA__ in category pages."""
    urls = []
    seen = set()

    for category in ["reviews", "columns"]:
        next_link = f"https://www.stereogum.com/category/{category}/"
        while next_link:
            html = fetch_live(next_link)
            if not html:
                break

            match = re.search(
                r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
                html,
            )
            if not match:
                break

            data = json.loads(match.group(1))
            props = data.get("props", {}).get("pageProps", {})
            posts = props.get("posts", [])

            if not posts:
                break

            new = 0
            for post in posts:
                link = post.get("link", "")
                if link and link not in seen:
                    seen.add(link)
                    urls.append(link)
                    new += 1

            next_page = props.get("nextPageLink", "")
            log.info(
                "Stereogum %s: %d new (%d total), next=%s",
                category,
                new,
                len(urls),
                bool(next_page),
            )

            if not next_page or new == 0:
                break

            next_link = (
                next_page
                if next_page.startswith("http")
                else f"https://www.stereogum.com{next_page}"
            )
            time.sleep(1.5)

    return urls


def crawl_stereogum():
    """Download Stereogum reviews."""
    output = DATA_DIR / "stereogum" / "reviews.jsonl"
    url_cache = DATA_DIR / "stereogum" / "urls.txt"
    done = load_done(output)

    if url_cache.exists():
        with open(url_cache) as f:
            urls = [line.strip() for line in f if line.strip()]
        log.info("Stereogum: loaded %d cached URLs", len(urls))
    else:
        log.info("Stereogum: discovering review URLs...")
        urls = discover_stereogum_urls()
        with open(url_cache, "w") as f:
            f.write("\n".join(urls))
        log.info("Stereogum: found %d review URLs", len(urls))

    log.info("Stereogum: %d to process, %d already done", len(urls), len(done))

    success = 0
    errors = 0

    with open(output, "a") as out:
        for i, url in enumerate(urls):
            if url in done:
                continue

            html = fetch_live(url)
            if not html:
                errors += 1
                time.sleep(1.5)
                continue

            extracted = extract_text(html, url)
            if extracted and extracted.get("text"):
                record = {
                    "url": url,
                    "title": extracted.get("title", ""),
                    "author": extracted.get("author", ""),
                    "date": extracted.get("date", ""),
                    "text": extracted["text"],
                    "source": "stereogum",
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()
                success += 1
                done.add(url)
            else:
                errors += 1

            if (success + errors) % 50 == 0:
                log.info(
                    "Stereogum progress: %d/%d | %d extracted, %d errors",
                    i + 1,
                    len(urls),
                    success,
                    errors,
                )

            time.sleep(1.5)

    log.info("Stereogum done: %d extracted, %d errors", success, errors)


CRAWLERS = {
    "tmt": crawl_tmt,
    "quietus": crawl_quietus,
    "bandcamp": crawl_bandcamp_daily,
    "stereogum": crawl_stereogum,
}


def main():
    parser = argparse.ArgumentParser(description="Download music reviews")
    parser.add_argument("source", choices=list(CRAWLERS.keys()), help="Review source to crawl")
    args = parser.parse_args()
    CRAWLERS[args.source]()


if __name__ == "__main__":
    main()
