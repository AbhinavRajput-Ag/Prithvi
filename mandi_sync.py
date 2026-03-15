"""
mandi_sync.py — Standalone Agmarknet price fetcher for Prithvi

Fetches wheat, soybean, rice prices for Madhya Pradesh from data.gov.in
and posts them to the Prithvi API.

Usage:
    python mandi_sync.py               # sync once
    python mandi_sync.py --dry-run     # fetch but don't store
    python mandi_sync.py --watch 3600  # sync every 3600 seconds

Environment variables required:
    PRITHVI_API_URL     e.g. https://prithvi-s41b.onrender.com
    PRITHVI_USERNAME    admin username
    PRITHVI_PASSWORD    admin password
    AGMARKNET_API_KEY   your data.gov.in API key
"""

import argparse
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AGMARKNET_URL = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"

TARGETS = [
    {"commodity": "Wheat",    "crop_type": "wheat",   "state": "Madhya Pradesh"},
    {"commodity": "Soyabean", "crop_type": "soybean", "state": "Madhya Pradesh"},
    {"commodity": "Rice",     "crop_type": "rice",    "state": "Madhya Pradesh"},
]


def get_env(key):
    val = os.getenv(key, "")
    if not val:
        raise ValueError(f"Environment variable {key} is not set")
    return val


def fetch_records(commodity: str, state: str, api_key: str, limit: int = 100) -> list:
    params = urllib.parse.urlencode({
        "api-key": api_key,
        "format": "json",
        "limit": limit,
        "filters[state.keyword]": state,
        "filters[commodity]": commodity,
    })
    url = f"{AGMARKNET_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "Prithvi/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    records = data.get("records", [])
    logger.info("Fetched %d records for %s / %s", len(records), commodity, state)
    return records


def get_token(api_url: str, username: str, password: str) -> str:
    payload = json.dumps({"username": username, "password": password}).encode()
    req = urllib.request.Request(
        f"{api_url}/auth/login",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode())
    return data["access_token"]


def post_snapshot(api_url: str, token: str, entry: dict) -> dict:
    payload = json.dumps(entry).encode()
    req = urllib.request.Request(
        f"{api_url}/mandi-prices/add",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def parse_date(raw: str) -> str:
    """Convert DD/MM/YYYY → YYYY-MM-DD."""
    if not raw:
        return str(date.today())
    parts = raw.split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return raw


def sync_once(dry_run: bool = False):
    api_key  = get_env("AGMARKNET_API_KEY")
    api_url  = get_env("PRITHVI_API_URL").rstrip("/")
    username = get_env("PRITHVI_USERNAME")
    password = get_env("PRITHVI_PASSWORD")

    token = None
    if not dry_run:
        logger.info("Logging in to Prithvi API...")
        token = get_token(api_url, username, password)

    total_inserted = 0

    for target in TARGETS:
        try:
            records = fetch_records(target["commodity"], target["state"], api_key)
        except Exception as exc:
            logger.error("Fetch failed for %s: %s", target["commodity"], exc)
            continue

        if dry_run:
            logger.info("[dry-run] Would insert %d records for %s", len(records), target["commodity"])
            continue

        inserted = 0
        for rec in records:
            entry = {
                "crop_type":    target["crop_type"],
                "variety":      rec.get("variety"),
                "market_name":  rec.get("market"),
                "district":     rec.get("district"),
                "state":        rec.get("state"),
                "snapshot_date": parse_date(rec.get("arrival_date", "")),
                "min_price":    rec.get("min_price"),
                "modal_price":  rec.get("modal_price"),
                "max_price":    rec.get("max_price"),
                "source_name":  "agmarknet",
                "raw_payload":  json.dumps(rec),
            }
            try:
                post_snapshot(api_url, token, entry)
                inserted += 1
            except Exception as exc:
                logger.warning("Insert failed for record: %s — %s", rec.get("market"), exc)

        logger.info("%s: %d / %d inserted", target["commodity"], inserted, len(records))
        total_inserted += inserted

    logger.info("Sync complete. Total inserted: %d", total_inserted)
    return total_inserted


def main():
    parser = argparse.ArgumentParser(description="Sync Agmarknet mandi prices into Prithvi")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but do not store")
    parser.add_argument("--watch", type=int, default=0, metavar="SECONDS",
                        help="Run continuously every N seconds")
    args = parser.parse_args()

    if args.watch:
        logger.info("Watch mode: syncing every %ds", args.watch)
        while True:
            try:
                sync_once(dry_run=args.dry_run)
            except Exception as exc:
                logger.error("Sync error: %s", exc)
            logger.info("Sleeping %ds...", args.watch)
            time.sleep(args.watch)
    else:
        sync_once(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
