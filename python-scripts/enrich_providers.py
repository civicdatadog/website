"""
enrich_providers.py

Enrich CCAP providers with Google Places data (website, phone, GPS, etc.).
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "python-scripts" / "data"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value


def read_providers(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def write_enriched(path: Path, rows: Iterable[Dict[str, str]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def build_query(row: Dict[str, str]) -> str:
    parts = [
        row.get("provider_name", ""),
        row.get("address", ""),
        row.get("city", ""),
        row.get("state", ""),
        row.get("zip", ""),
    ]
    return ", ".join([p for p in parts if p])


def normalize_address_key(row: Dict[str, str]) -> str:
    parts = [
        row.get("address", ""),
        row.get("city", ""),
        row.get("state", ""),
        row.get("zip", ""),
    ]
    key = " ".join(parts).lower()
    key = "".join(ch for ch in key if ch.isalnum() or ch.isspace())
    key = " ".join(key.split())
    if not key:
        name = row.get("provider_name", "").lower()
        name = "".join(ch for ch in name if ch.isalnum() or ch.isspace())
        key = " ".join(name.split())
    return key


def extract_places_fields(row: Dict[str, str]) -> Dict[str, str]:
    return {k: v for k, v in row.items() if k.startswith("places_")}


def places_text_search(session: requests.Session, api_key: str, query: str) -> Optional[Dict[str, str]]:
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    resp = session.get(url, params={"query": query, "key": api_key}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK":
        return None
    return data.get("results", [None])[0]


def places_details(session: requests.Session, api_key: str, place_id: str) -> Optional[Dict[str, str]]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join(
        [
            "place_id",
            "name",
            "formatted_address",
            "geometry/location",
            "website",
            "formatted_phone_number",
            "international_phone_number",
            "types",
            "business_status",
            "url",
            "rating",
            "user_ratings_total",
        ]
    )
    resp = session.get(
        url,
        params={"place_id": place_id, "fields": fields, "key": api_key},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK":
        return None
    return data.get("result")


def enrich_row(
    session: requests.Session,
    api_key: str,
    row: Dict[str, str],
    sleep_seconds: float,
) -> Dict[str, str]:
    query = build_query(row)
    result = places_text_search(session, api_key, query)
    time.sleep(sleep_seconds)
    if not result:
        row["places_query"] = query
        row["places_status"] = "NO_MATCH"
        return row

    place_id = result.get("place_id", "")
    details = places_details(session, api_key, place_id) if place_id else None
    time.sleep(sleep_seconds)

    row["places_query"] = query
    row["places_place_id"] = place_id
    row["places_name"] = result.get("name", "")
    row["places_formatted_address"] = result.get("formatted_address", "")
    row["places_business_status"] = result.get("business_status", "")
    row["places_rating"] = str(result.get("rating", ""))
    row["places_user_ratings_total"] = str(result.get("user_ratings_total", ""))

    if details:
        location = details.get("geometry", {}).get("location", {})
        row["places_lat"] = str(location.get("lat", ""))
        row["places_lng"] = str(location.get("lng", ""))
        row["places_website"] = details.get("website", "")
        row["places_phone"] = details.get("formatted_phone_number", "")
        row["places_intl_phone"] = details.get("international_phone_number", "")
        row["places_types"] = ",".join(details.get("types", []) or [])
        row["places_url"] = details.get("url", "")
        row["places_details_name"] = details.get("name", "")
        row["places_details_address"] = details.get("formatted_address", "")
        row["places_details_status"] = details.get("business_status", "")
        row["places_details_rating"] = str(details.get("rating", ""))
        row["places_details_user_ratings_total"] = str(details.get("user_ratings_total", ""))
        row["places_status"] = "OK"
    else:
        row["places_status"] = "NO_DETAILS"
    return row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich providers with Google Places.")
    parser.add_argument(
        "--input",
        default=str(DATA_DIR / "mn_ccap_providers_all.csv"),
        help="Input providers CSV",
    )
    parser.add_argument(
        "--output",
        default=str(DATA_DIR / "mn_ccap_providers_enriched.csv"),
        help="Output enriched CSV",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep seconds between API calls (default: 0.2)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of rows (0 = no limit)",
    )
    return parser.parse_args()


def main() -> int:
    load_env_file(ROOT / ".env")
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        print("ERROR: GOOGLE_API_KEY not set.")
        return 1

    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        return 1

    rows = read_providers(input_path)
    if args.limit:
        rows = rows[: args.limit]

    session = requests.Session()
    enriched_rows = []
    cache: Dict[str, Dict[str, str]] = {}
    for idx, row in enumerate(rows, start=1):
        print(f"[{idx}/{len(rows)}] {row.get('provider_name','')}")
        cache_key = normalize_address_key(row)
        if cache_key and cache_key in cache:
            cached_fields = cache[cache_key]
            row.update(cached_fields)
            row["places_status"] = "CACHED"
            enriched_rows.append(row)
            continue

        enriched = enrich_row(session, api_key, row, args.sleep)
        if cache_key:
            cache[cache_key] = extract_places_fields(enriched)
        enriched_rows.append(enriched)

    fieldnames = sorted({k for row in enriched_rows for k in row.keys()})
    output_path = Path(args.output)
    write_enriched(output_path, enriched_rows, fieldnames)
    print(f"Wrote enriched CSV to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
