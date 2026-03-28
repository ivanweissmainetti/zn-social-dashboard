#!/usr/bin/env python3
"""
ZN Consulting — Social Media Data Fetcher
==========================================
Fetches social media data via RSSHub and the Bluesky AT Protocol API,
then saves JSON files for the dashboard.

Dependencies: pip install requests pyyaml feedparser
"""

import argparse
import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
import yaml

# ---------------------------------------------------------------------------
#  Config
# ---------------------------------------------------------------------------
RSSHUB_INSTANCES = [
    os.environ.get("RSSHUB_INSTANCE", "https://rsshub.app"),
    "https://rsshub.rssforever.com",
    "https://rsshub.moeyy.xyz",
]

BLUESKY_API = "https://public.api.bsky.app/xrpc"

REQUEST_TIMEOUT = 10  # seconds
RATE_LIMIT_DELAY = 1  # seconds between requests

POSITIVE_WORDS = {
    "great", "amazing", "love", "excellent", "fantastic", "wonderful", "best",
    "congratulations", "impressive", "outstanding", "brilliant", "superb",
    "happy", "proud", "thrilled", "excited", "innovative", "remarkable",
}
NEGATIVE_WORDS = {
    "bad", "terrible", "worst", "hate", "awful", "disappointing", "poor",
    "fail", "disaster", "horrible", "broken", "trash", "scam", "fraud",
    "angry", "frustrated", "unacceptable", "disgusting",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetcher")

# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def sentiment(text: str) -> str:
    """Simple keyword-based sentiment analysis."""
    if not text:
        return "neutral"
    words = set(text.lower().split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    if pos > neg:
        return "positive"
    elif neg > pos:
        return "negative"
    return "neutral"


def fetch_url(url: str) -> requests.Response | None:
    """Fetch a URL with timeout and error handling."""
    try:
        log.info(f"  Fetching: {url[:100]}...")
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
            "User-Agent": "ZNConsulting-SocialDashboard/1.0"
        })
        r.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)
        return r
    except Exception as e:
        log.warning(f"  Failed to fetch {url[:80]}: {e}")
        return None


def fetch_rsshub(route: str) -> list[dict]:
    """Try fetching an RSSHub route from multiple instances."""
    for instance in RSSHUB_INSTANCES:
        url = instance.rstrip("/") + "/" + route.lstrip("/")
        resp = fetch_url(url)
        if resp is None:
            continue
        try:
            feed = feedparser.parse(resp.text)
            items = []
            for entry in feed.entries:
                items.append({
                    "content": entry.get("title", "") or entry.get("summary", ""),
                    "date": _parse_date(entry),
                    "url": entry.get("link", ""),
                })
            if items:
                log.info(f"  Got {len(items)} items from {instance}")
                return items
        except Exception as e:
            log.warning(f"  Parse error for {instance}: {e}")
    return []


def _parse_date(entry) -> str:
    """Extract a date from a feedparser entry, fallback to now."""
    for field in ("published_parsed", "updated_parsed"):
        t = entry.get(field)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
            except Exception:
                pass
    for field in ("published", "updated"):
        v = entry.get(field)
        if v:
            return v
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
#  Platform fetchers
# ---------------------------------------------------------------------------

def fetch_linkedin(config: dict) -> tuple[list, list]:
    """Fetch LinkedIn company posts via RSSHub."""
    cid = config.get("company_id")
    if not cid:
        return [], []
    route = f"linkedin/company/{cid}/posts"
    raw = fetch_rsshub(route)
    posts = []
    for i, item in enumerate(raw[:20]):
        posts.append({
            "id": f"li_{i}",
            "content": item["content"],
            "date": item["date"],
            "likes": 0,
            "comments": 0,
            "reposts": 0,
            "type": "post",
            "url": item["url"],
        })
    return posts, []


def fetch_instagram(config: dict) -> tuple[list, list]:
    """Fetch Instagram posts via RSSHub Picnob route."""
    username = config.get("username")
    if not username:
        return [], []
    route = f"picnob/user/{username}"
    raw = fetch_rsshub(route)
    posts = []
    for i, item in enumerate(raw[:20]):
        posts.append({
            "id": f"ig_{i}",
            "content": item["content"],
            "date": item["date"],
            "likes": 0,
            "comments": 0,
            "reposts": 0,
            "type": "post",
            "url": item["url"],
        })
    return posts, []


def fetch_x(config: dict) -> tuple[list, list]:
    """Fetch X/Twitter posts via RSSHub."""
    username = config.get("username")
    if not username:
        return [], []
    route = f"twitter/user/{username}"
    raw = fetch_rsshub(route)
    posts = []
    for i, item in enumerate(raw[:20]):
        posts.append({
            "id": f"x_{i}",
            "content": item["content"],
            "date": item["date"],
            "likes": 0,
            "comments": 0,
            "reposts": 0,
            "type": "post",
            "url": item["url"],
        })
    return posts, []


def fetch_bluesky(config: dict, keywords: list[str]) -> tuple[list, list]:
    """Fetch Bluesky posts and mentions via the AT Protocol API (free)."""
    handle = config.get("handle")
    if not handle:
        return [], []

    posts = []
    mentions = []

    # 1. Fetch author's own posts
    url = f"{BLUESKY_API}/app.bsky.feed.getAuthorFeed?actor={handle}&limit=30"
    resp = fetch_url(url)
    if resp:
        try:
            data = resp.json()
            for i, item in enumerate(data.get("feed", [])):
                post = item.get("post", {})
                record = post.get("record", {})
                posts.append({
                    "id": f"bs_{i}",
                    "content": record.get("text", ""),
                    "date": record.get("createdAt", ""),
                    "likes": post.get("likeCount", 0),
                    "comments": post.get("replyCount", 0),
                    "reposts": post.get("repostCount", 0),
                    "type": "post",
                    "url": f"https://bsky.app/profile/{handle}/post/{post.get('uri', '').split('/')[-1]}",
                })
        except Exception as e:
            log.warning(f"  Bluesky feed parse error: {e}")

    # 2. Search for keyword mentions
    for kw in keywords[:3]:  # limit keyword searches
        url = f"{BLUESKY_API}/app.bsky.feed.searchPosts?q={requests.utils.quote(kw)}&limit=20"
        resp = fetch_url(url)
        if not resp:
            continue
        try:
            data = resp.json()
            for item in data.get("posts", []):
                author_handle = item.get("author", {}).get("handle", "")
                if author_handle == handle:
                    continue  # skip own posts
                text = item.get("record", {}).get("text", "")
                mentions.append({
                    "content": text,
                    "date": item.get("record", {}).get("createdAt", ""),
                    "author": item.get("author", {}).get("displayName", author_handle),
                    "sentiment": sentiment(text),
                    "url": f"https://bsky.app/profile/{author_handle}/post/{item.get('uri', '').split('/')[-1]}",
                })
        except Exception as e:
            log.warning(f"  Bluesky search parse error for '{kw}': {e}")

    return posts, mentions


# ---------------------------------------------------------------------------
#  Build output
# ---------------------------------------------------------------------------

def build_engagement_timeline(platforms: dict) -> list[dict]:
    """Build a 30-day engagement timeline from available data."""
    now = datetime.now(timezone.utc)
    days = {}
    for i in range(30, -1, -1):
        d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        days[d] = {"date": d, "linkedin": 0, "instagram": 0, "x": 0, "bluesky": 0}

    for pkey, pdata in platforms.items():
        if pkey not in days[list(days.keys())[0]]:
            continue
        for post in pdata.get("posts", []):
            try:
                post_date = post["date"][:10]
                if post_date in days:
                    eng = (post.get("likes", 0) + post.get("comments", 0) + post.get("reposts", 0))
                    days[post_date][pkey] += eng
            except Exception:
                pass

    return list(days.values())


def process_client(client: dict) -> dict:
    """Process a single client: fetch all platforms, build output JSON."""
    name = client["name"]
    slug = client["slug"]
    keywords = client.get("keywords", [name])

    log.info(f"\n{'='*60}")
    log.info(f"Processing: {name} (slug: {slug})")
    log.info(f"{'='*60}")

    platforms_config = client.get("platforms", {})
    platforms_data = {}

    # LinkedIn
    if "linkedin" in platforms_config:
        log.info("LinkedIn:")
        posts, mentions = fetch_linkedin(platforms_config["linkedin"])
        platforms_data["linkedin"] = {
            "handle": f"@{platforms_config['linkedin'].get('company_id', '')}",
            "posts": posts,
            "mentions": mentions,
        }

    # Instagram
    if "instagram" in platforms_config:
        log.info("Instagram:")
        posts, mentions = fetch_instagram(platforms_config["instagram"])
        platforms_data["instagram"] = {
            "handle": f"@{platforms_config['instagram'].get('username', '')}",
            "posts": posts,
            "mentions": mentions,
        }

    # X/Twitter
    if "x" in platforms_config:
        log.info("X/Twitter:")
        posts, mentions = fetch_x(platforms_config["x"])
        platforms_data["x"] = {
            "handle": f"@{platforms_config['x'].get('username', '')}",
            "posts": posts,
            "mentions": mentions,
        }

    # Bluesky
    if "bluesky" in platforms_config:
        log.info("Bluesky:")
        posts, mentions = fetch_bluesky(platforms_config["bluesky"], keywords)
        platforms_data["bluesky"] = {
            "handle": platforms_config["bluesky"].get("handle", ""),
            "posts": posts,
            "mentions": mentions,
        }

    # Fill in empty platforms
    for p in ("linkedin", "instagram", "x", "bluesky"):
        if p not in platforms_data:
            platforms_data[p] = {"handle": "", "posts": [], "mentions": []}

    output = {
        "client_name": name,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "platforms": platforms_data,
        "engagement_timeline": build_engagement_timeline(platforms_data),
    }

    return output


def print_summary(slug: str, data: dict):
    """Print a summary of fetched data."""
    total_posts = sum(len(p.get("posts", [])) for p in data["platforms"].values())
    total_mentions = sum(len(p.get("mentions", [])) for p in data["platforms"].values())
    log.info(f"\n  Summary for {data['client_name']}:")
    log.info(f"    Total posts:    {total_posts}")
    log.info(f"    Total mentions: {total_mentions}")
    for pname, pdata in data["platforms"].items():
        log.info(f"    {pname:12s}: {len(pdata.get('posts',[]))} posts, {len(pdata.get('mentions',[]))} mentions")


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ZN Consulting Social Media Data Fetcher")
    parser.add_argument("--config", required=True, help="Path to clients.yaml config file")
    args = parser.parse_args()

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        log.error(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f)

    clients = config.get("clients", [])
    if not clients:
        log.error("No clients found in config")
        sys.exit(1)

    # Determine output directory
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir.parent / "data"
    data_dir.mkdir(exist_ok=True)

    log.info(f"Found {len(clients)} client(s) to process")
    log.info(f"Output directory: {data_dir}")

    errors = []
    for client in clients:
        try:
            data = process_client(client)
            slug = client["slug"]

            # Save JSON
            out_path = data_dir / f"{slug}.json"
            with open(out_path, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            log.info(f"  Saved: {out_path}")
            print_summary(slug, data)

        except Exception as e:
            log.error(f"  Error processing {client.get('name', '?')}: {e}")
            errors.append(client.get("name", "?"))

    # Final report
    log.info(f"\n{'='*60}")
    log.info(f"Done! Processed {len(clients)} client(s).")
    if errors:
        log.warning(f"Errors with: {', '.join(errors)}")
    else:
        log.info("All clients processed successfully.")


if __name__ == "__main__":
    main()
