"""
mn_ccap_scraper.py

Static-friendly helper script to download and normalize Minnesota
Child Care Assistance Program (CCAP) providers from official sources.

Design goals:
- Prefer CSV exports / official downloads over HTML scraping
- Be polite (no aggressive crawling, respect robots.txt and rate limits)
- Produce a clean CSV in ../python-scripts/data/mn_ccap_providers.csv

NOTE:
- The Minnesota DHS Licensing Information Lookup lives at:
    https://licensinglookup.dhs.state.mn.us/
- That site exposes search + export functionality that may change over time.
- This script is written so you can easily plug in the **official CSV export URL**
  once you confirm it in the browser / dev tools.
"""

import argparse
import csv
import html
import json
import os
import pathlib
import re
import sys
import time
import urllib.parse
from datetime import UTC, datetime
from typing import Iterable, Dict, Any, Optional

import requests


ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "python-scripts" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Where we keep a copy of the raw CSV exactly as downloaded from DHS.
RAW_CSV_PATH = DATA_DIR / "mn_ccap_raw.csv"

# Placeholder for the official CCAP / child-care licensing CSV export.
# You can either set the MN_CCAP_EXPORT_URL environment variable or
# hardcode the URL below. You will need to:
#   1. Go to https://licensinglookup.dhs.state.mn.us/
#   2. Run the search you want (e.g., all Child Care Center + Family Child Care)
#   3. Use your browser's Network tab to find the actual CSV download request
#   4. Paste that full URL (and any required query parameters) below.
CSV_EXPORT_URL: Optional[str] = os.getenv("MN_CCAP_EXPORT_URL") or None
CSV_EXPORT_HAR: Optional[str] = os.getenv("MN_CCAP_EXPORT_HAR") or None


def load_har_export(
    har_path: pathlib.Path,
) -> tuple[str, str, Dict[str, str], Dict[str, str], Optional[str]]:
    """
    Pull the CSV export request details from a HAR file.
    """
    data = json.loads(har_path.read_text(encoding="utf-8"))
    entries = data.get("log", {}).get("entries", [])
    for entry in entries:
        req = entry.get("request", {})
        res = entry.get("response", {})
        headers = {h.get("name", ""): h.get("value", "") for h in res.get("headers", [])}
        content_type = headers.get("Content-Type", "")
        if not content_type:
            content_type = res.get("content", {}).get("mimeType", "")
        if "text/csv" not in content_type.lower():
            continue

        url = req.get("url", "")
        if not url:
            continue

        req_headers = {h.get("name", ""): h.get("value", "") for h in req.get("headers", [])}
        cookies = {c.get("name", ""): c.get("value", "") for c in req.get("cookies", [])}

        # Drop headers that requests should manage.
        for key in ["Host", "Content-Length", "Connection", "Cookie"]:
            req_headers.pop(key, None)
        for key in list(req_headers.keys()):
            if key.startswith(":"):
                req_headers.pop(key, None)

        method = req.get("method", "GET").upper()
        post_data = req.get("postData", {}).get("text")
        return method, url, req_headers, cookies, post_data

    raise ValueError("No CSV export response found in HAR.")


def download_csv(
    url: str,
    *,
    method: str = "GET",
    data: Optional[str] = None,
    timeout: int = 60,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
) -> str:
    """
    Download the raw CSV from the official Minnesota DHS export endpoint.

    Returns the CSV text. Raises for HTTP and network errors.
    """
    base_headers = {
        "User-Agent": "CivicDatadog-CCAP-Scraper/1.0 (https://civicdatadog.com)",
        "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    }
    merged_headers = dict(base_headers)
    if headers:
        merged_headers.update(headers)
    resp = requests.request(
        method,
        url,
        headers=merged_headers,
        cookies=cookies,
        data=data,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.text


def parse_providers(csv_text: str) -> Iterable[Dict[str, Any]]:
    """
    Parse the Minnesota DHS licensing CSV into a normalized provider schema.

    Because DHS can change column names over time, this function maps from a few
    common patterns (e.g. 'License Holder Name', 'Provider Name', 'City', etc.).
    Adjust the column mapping below once you inspect a real export.
    """
    reader = csv.DictReader(csv_text.splitlines())

    # Map DHS column names -> our normalized schema
    # Update these keys after looking at an actual CSV header row.
    column_map = {
        "LicenseNumber": "license_number",
        "License #": "license_number",
        "License Holder Name": "provider_name",
        "Provider Name": "provider_name",
        "Doing Business As": "doing_business_as",
        "DBA Name": "doing_business_as",
        "License Type": "license_type",
        "City": "city",
        "State": "state",
        "Zip": "zip",
        "County": "county",
        "Status": "status",
        "License Status": "status",
        "Address": "address",
        "Street Address": "address",
    }

    for row in reader:
        normalized: Dict[str, Any] = {
            "raw_row": row,  # keep the raw for debugging/future fields
        }

        for src_col, norm_col in column_map.items():
            if src_col in row and row[src_col]:
                normalized[norm_col] = row[src_col].strip()

        # Ensure some core fields exist even if empty
        normalized.setdefault("license_number", "")
        normalized.setdefault("provider_name", "")
        normalized.setdefault("license_type", "")
        normalized.setdefault("city", "")
        normalized.setdefault("state", "MN")
        normalized.setdefault("zip", "")
        normalized.setdefault("county", "")
        normalized.setdefault("status", "")
        normalized.setdefault("address", "")

        yield normalized


def write_normalized_csv(providers: Iterable[Dict[str, Any]], out_path: pathlib.Path) -> None:
    """
    Write normalized provider records to a CSV file suitable for downstream static tables.
    """
    out_fields = [
        "license_number",
        "provider_name",
        "doing_business_as",
        "license_type",
        "status",
        "address",
        "city",
        "state",
        "zip",
        "county",
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        for p in providers:
            writer.writerow({field: p.get(field, "") for field in out_fields})


def _clean_html_block(text: str) -> list[str]:
    text = re.sub(r"<br\\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines


def parse_providers_html(html_text: str) -> Iterable[Dict[str, Any]]:
    """
    Parse the DHS Results.aspx HTML into a normalized provider schema.
    """
    pattern = re.compile(
        r'<table border="0" class="LicTable1">.*?'
        r'class="LicTitle1"[^>]*>\\s*<a[^>]*>(.*?)</a>.*?'
        r'class="LicStatus1"[^>]*>\\s*(.*?)</td>.*?'
        r'<table\\s+border="0"\\s+class="LicTable">.*?'
        r'class="LicContentL"[^>]*>\\s*(.*?)</td>.*?'
        r'class="LicContentR"[^>]*>\\s*(.*?)</td>',
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(html_text):
        provider_name = html.unescape(match.group(1)).strip()
        status = html.unescape(match.group(2)).strip()
        left_block = match.group(3)
        right_block = match.group(4)

        left_lines = _clean_html_block(left_block)
        right_text = html.unescape(re.sub(r"<[^>]+>", "", right_block))

        address = left_lines[0] if left_lines else ""
        city = state = zip_code = ""
        county = ""
        if len(left_lines) >= 2:
            city_state_zip = left_lines[1]
            m = re.match(r"^(.*?),\\s*([A-Z]{2})\\s+(\\d{5})", city_state_zip)
            if m:
                city, state, zip_code = m.group(1).strip(), m.group(2), m.group(3)
        for line in left_lines[2:]:
            if line.endswith("County"):
                county = line.replace("County", "").strip()

        license_number = ""
        license_type = ""
        m = re.search(r"License number:\\s*([A-Za-z0-9-]+)", right_text)
        if m:
            license_number = m.group(1).strip()
        m = re.search(r"Type of service:\\s*(.+)", right_text)
        if m:
            license_type = m.group(1).strip()

        normalized: Dict[str, Any] = {
            "raw_row": {
                "left_block": left_block,
                "right_block": right_block,
            },
            "license_number": license_number,
            "provider_name": provider_name,
            "license_type": license_type,
            "status": status,
            "address": address,
            "city": city,
            "state": state or "MN",
            "zip": zip_code,
            "county": county,
        }
        yield normalized


def extract_hidden_fields(html_text: str) -> Dict[str, str]:
    def extract(name: str) -> str:
        pattern = rf'name="{re.escape(name)}"[^>]*value="(.*?)"'
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if not match:
            return ""
        return html.unescape(match.group(1))

    return {
        "__VIEWSTATE": extract("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": extract("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": extract("__EVENTVALIDATION"),
    }


def build_url_with_zip(url: str, zip_code: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    query["z"] = zip_code
    new_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


def download_csv_for_zip(
    session: requests.Session,
    base_url: str,
    zip_code: str,
    headers: Dict[str, str],
    timeout: int,
) -> str:
    url = build_url_with_zip(base_url, zip_code)
    get_resp = session.get(url, headers=headers, timeout=timeout)
    get_resp.raise_for_status()
    hidden = extract_hidden_fields(get_resp.text)
    if not hidden["__VIEWSTATE"] or not hidden["__EVENTVALIDATION"]:
        raise RuntimeError("Missing hidden fields required for export.")

    form_data = {
        "__EVENTTARGET": "csvdownload",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": hidden["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": hidden["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION": hidden["__EVENTVALIDATION"],
        "__SCROLLPOSITIONX": "0",
        "__SCROLLPOSITIONY": "0",
    }
    post_resp = session.post(url, headers=headers, data=form_data, timeout=timeout)
    post_resp.raise_for_status()
    return post_resp.text


def download_html_for_zip(
    session: requests.Session,
    base_url: str,
    zip_code: str,
    headers: Dict[str, str],
    timeout: int,
) -> str:
    url = build_url_with_zip(base_url, zip_code)
    get_resp = session.get(url, headers=headers, timeout=timeout)
    get_resp.raise_for_status()
    return get_resp.text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download and normalize MN DHS licensing data.")
    parser.add_argument("url", nargs="?", help="Direct CSV export URL")
    parser.add_argument("--har", dest="har_path", help="Path to HAR file containing CSV export request")
    parser.add_argument(
        "--zips",
        dest="zips_path",
        help="Path to newline-delimited zip codes file for batch export",
    )
    parser.add_argument(
        "--html",
        dest="use_html",
        action="store_true",
        help="Parse HTML results pages instead of CSV export in batch mode",
    )
    parser.add_argument(
        "--html-file",
        dest="html_file",
        help="Parse a saved Results.aspx HTML file",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=3.0,
        help="Seconds to sleep between zip downloads (default: 3.0)",
    )
    return parser.parse_args()


def main(
    url: Optional[str] = None,
    har_path: Optional[str] = None,
    zips_path: Optional[str] = None,
    sleep_seconds: float = 3.0,
    use_html: bool = False,
    html_file: Optional[str] = None,
) -> int:
    """
    Entry point for the scraper.

    Usage:
        # Online mode (if DHS allows direct CSV export):
        python mn_ccap_scraper.py "https://licensinglookup.dhs.state.mn.us/...export...csv..."

        # Offline mode (if you manually downloaded the CSV to mn_ccap_raw.csv):
        python mn_ccap_scraper.py
    """
    csv_text: Optional[str] = None

    if html_file:
        html_path = pathlib.Path(html_file)
        if not html_path.exists():
            sys.stderr.write(f"ERROR: HTML file not found: {html_path}\n")
            return 1
        html_text = html_path.read_text(encoding="utf-8")
        providers = list(parse_providers_html(html_text))
        out_path = DATA_DIR / "mn_ccap_providers.html.csv"
        write_normalized_csv(providers, out_path)
        print(f"Wrote normalized providers to {out_path.relative_to(ROOT)}")
        return 0

    if zips_path:
        effective_har = pathlib.Path(har_path or CSV_EXPORT_HAR or "")
        if not effective_har.exists():
            sys.stderr.write("ERROR: Batch mode requires a HAR file with export details.\n")
            return 1

        zips_file = pathlib.Path(zips_path)
        if not zips_file.exists():
            sys.stderr.write(f"ERROR: Zip list file not found: {zips_file}\n")
            return 1

        zip_codes = [z.strip() for z in zips_file.read_text(encoding="utf-8").splitlines() if z.strip()]
        if not zip_codes:
            sys.stderr.write(f"ERROR: No zip codes found in {zips_file}\n")
            return 1

        export_method, export_url, export_headers, export_cookies, _ = load_har_export(effective_har)
        if not use_html and export_method != "POST":
            sys.stderr.write("ERROR: Expected POST export from HAR; got different method.\n")
            return 1

        session = requests.Session()
        session.headers.update(export_headers)
        session.cookies.update(export_cookies)

        combined_providers: list[Dict[str, Any]] = []
        for idx, zip_code in enumerate(zip_codes, start=1):
            print(f"[{idx}/{len(zip_codes)}] Exporting zip {zip_code}…")
            try:
                if use_html:
                    html_text = download_html_for_zip(
                        session,
                        export_url,
                        zip_code,
                        headers=export_headers,
                        timeout=60,
                    )
                else:
                    csv_text = download_csv_for_zip(
                        session,
                        export_url,
                        zip_code,
                        headers=export_headers,
                        timeout=60,
                    )
            except Exception as exc:
                sys.stderr.write(f"ERROR: Failed zip {zip_code}: {exc}\n")
                continue

            if use_html:
                if html_text.lstrip().startswith("<!DOCTYPE html>") is False:
                    sys.stderr.write(f"ERROR: Zip {zip_code} did not return HTML.\n")
                    continue
                raw_path = DATA_DIR / f"mn_ccap_raw_{zip_code}.html"
                raw_path.write_text(html_text, encoding="utf-8")
                providers = list(parse_providers_html(html_text))
            else:
                if csv_text.lstrip().startswith("<!DOCTYPE html>"):
                    sys.stderr.write(f"ERROR: Zip {zip_code} returned HTML, not CSV.\n")
                    continue
                raw_path = DATA_DIR / f"mn_ccap_raw_{zip_code}.csv"
                raw_path.write_text(csv_text, encoding="utf-8")
                providers = list(parse_providers(csv_text))
            combined_providers.extend(providers)

            out_path = DATA_DIR / f"mn_ccap_providers_{zip_code}.csv"
            write_normalized_csv(providers, out_path)
            print(f"Wrote {out_path.relative_to(ROOT)}")

            if idx < len(zip_codes) and sleep_seconds > 0:
                time.sleep(sleep_seconds)

        combined_out = DATA_DIR / "mn_ccap_providers_all.csv"
        write_normalized_csv(combined_providers, combined_out)
        print(f"Wrote combined providers to {combined_out.relative_to(ROOT)}")
    elif har_path or CSV_EXPORT_HAR:
        effective_har = pathlib.Path(har_path or CSV_EXPORT_HAR)
        if not effective_har.exists():
            sys.stderr.write(f"ERROR: HAR file not found: {effective_har}\n")
            return 1
        print(f"Loading export request from HAR: {effective_har}")
        try:
            export_method, export_url, export_headers, export_cookies, export_data = load_har_export(
                effective_har
            )
        except ValueError as exc:
            sys.stderr.write(f"ERROR: {exc}\n")
            return 1

        now_utc = datetime.now(UTC)
        print(f"[{now_utc.isoformat()}] Downloading CCAP providers CSV…")
        csv_text = download_csv(
            export_url,
            method=export_method,
            data=export_data,
            headers=export_headers,
            cookies=export_cookies,
        )
        lines = csv_text.splitlines()
        print(f"Downloaded {len(lines)} lines of CSV.")

        RAW_CSV_PATH.write_text(csv_text, encoding="utf-8")
        print(f"Wrote raw CSV to {RAW_CSV_PATH.relative_to(ROOT)}")
    elif url or CSV_EXPORT_URL:
        # Online mode: download directly from DHS using the provided export URL
        effective_url = url or CSV_EXPORT_URL
        now_utc = datetime.now(UTC)
        print(f"[{now_utc.isoformat()}] Downloading CCAP providers CSV…")
        csv_text = download_csv(effective_url)
        lines = csv_text.splitlines()
        print(f"Downloaded {len(lines)} lines of CSV.")

        # Always write the raw CSV for inspection and debugging
        RAW_CSV_PATH.write_text(csv_text, encoding="utf-8")
        print(f"Wrote raw CSV to {RAW_CSV_PATH.relative_to(ROOT)}")
    else:
        # Offline mode: use a CSV file you downloaded manually in your browser
        if not RAW_CSV_PATH.exists():
            sys.stderr.write(
                "ERROR: No CSV URL provided and no local raw CSV found.\n"
                "Options:\n"
                "  A) Run with a real CSV export URL from DHS, e.g.:\n"
                "       python mn_ccap_scraper.py \"https://licensinglookup.dhs.state.mn.us/...export...csv...\"\n"
                "  B) Manually download the CSV in your browser and save it as:\n"
                f"       {RAW_CSV_PATH}\n"
                "     then re-run this script with no arguments.\n"
            )
            return 1

        csv_text = RAW_CSV_PATH.read_text(encoding="utf-8")
        # If the site returned a bot-protection HTML page, warn clearly
        if csv_text.lstrip().startswith("<!DOCTYPE html>"):
            sys.stderr.write(
                "ERROR: mn_ccap_raw.csv contains HTML (likely a CAPTCHA / bot-protection page), not CSV data.\n"
                "This happens when the DHS site blocks automated downloads.\n\n"
                "To proceed:\n"
                "  1) In your browser, solve any CAPTCHA and successfully download the real CSV file.\n"
                f"  2) Save that CSV over:\n"
                f"       {RAW_CSV_PATH}\n"
                "  3) Re-run:\n"
                "       python mn_ccap_scraper.py\n"
            )
            return 1

        print(f"Loaded local CSV from {RAW_CSV_PATH.relative_to(ROOT)}")

    print("Parsing and normalizing provider records…")
    providers = list(parse_providers(csv_text))
    print(f"Parsed {len(providers)} provider rows.")

    out_path = DATA_DIR / "mn_ccap_providers.csv"
    write_normalized_csv(providers, out_path)
    print(f"Wrote normalized providers to {out_path.relative_to(ROOT)}")

    return 0


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(
        main(args.url, args.har_path, args.zips_path, args.sleep, args.use_html, args.html_file)
    )
