"""
mn_ccap_selenium.py

Automates DHS Licensing Lookup CSV exports via a real Chrome session.
This script opens Chrome (non-headless by default), lets you solve any challenges,
and iterates over a zip list to download CSV exports.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import time
import urllib.parse
from typing import Iterable

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


DEFAULT_RESULTS_URL = (
    "https://licensinglookup.dhs.state.mn.us/Results.aspx?"
    "a=False&cdtrt=False&crfcc=False&crfmhc=False&e=0&dsfpv=False&hcbsbss=False&crfss=False&"
    "sils62=False&irts=False&qrtp61=False&crfsc=False&con=All&afcfads=False&ppy40=False&"
    "rsfsls=False&crsrc=False&ppy62=False&ppy61=False&dsfeds=False&sn=All&irtsrcs=False&"
    "co=All&cdtcwct=False&hcbsihss=False&crssls=False&hcbsics=False&locked=False&adcrem29=False&"
    "sils40=False&ci=All&hcbsds=False&crfgrs=False&crfts=False&rsfrs=false&hcbsrss=False&"
    "cdtsamht=False&hcbsses=False&hcbsiss=False&crfrt=False&crscr=False&crfprtf=False&"
    "cdtidat=False&stcse40=False&qrtp40=False&crsaost=False&cdtat=False&rcs40=False&dsfess=False&"
    "crfcdc=False&rcs=False&stcse62=False&stcse61=False&qrtp62=False&crfmhlock=False&dsfees=False&"
    "tn=All&z=55401&mhc=False&crfd=False&cdtnrt=False&sils61=False&s=All&afcaost=False&t=All&"
    "dsfdth=False&n=&l="
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Automate MN DHS CSV exports with Chrome.")
    parser.add_argument(
        "--zips",
        default="python-scripts/data/mn_zip_codes.txt",
        help="Path to newline-delimited zip codes file",
    )
    parser.add_argument(
        "--download-dir",
        default=str(pathlib.Path.home() / "Downloads"),
        help="Directory to save CSV downloads (default: ~/Downloads)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=3.0,
        help="Seconds to sleep between zips (default: 3.0)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Seconds to wait for page load and download (default: 120)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome in headless mode (default: false)",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_RESULTS_URL,
        help="Base Results.aspx URL to use; zip param will be replaced",
    )
    return parser.parse_args()


def build_url_with_zip(base_url: str, zip_code: str) -> str:
    parsed = urllib.parse.urlparse(base_url)
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    query["z"] = zip_code
    new_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


def load_zip_codes(path: str) -> list[str]:
    return [line.strip() for line in pathlib.Path(path).read_text().splitlines() if line.strip()]


def setup_driver(download_dir: str, headless: bool) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    prefs = {
        "download.default_directory": str(pathlib.Path(download_dir).resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=options)


def wait_for_download(download_dir: str, start_time: float, timeout: float) -> pathlib.Path:
    download_path = pathlib.Path(download_dir)
    while time.time() - start_time < timeout:
        candidates = []
        for path in download_path.glob("*"):
            try:
                mtime = path.stat().st_mtime
            except FileNotFoundError:
                continue
            candidates.append((mtime, path))
        candidates.sort(key=lambda item: item[0], reverse=True)
        for path in candidates:
            path = path[1]
            if path.suffix == ".crdownload":
                continue
            if path.stat().st_mtime >= start_time and path.is_file():
                return path
        time.sleep(0.5)
    raise TimeoutError("Download did not finish before timeout.")


def trigger_csv_export(driver: webdriver.Chrome) -> None:
    # Use the built-in ASP.NET postback if available.
    driver.execute_script(
        "if (typeof __doPostBack === 'function') {"
        "  __doPostBack('csvdownload','');"
        "} else {"
        "  var f = document.forms[0];"
        "  if (f) {"
        "    var et = document.createElement('input');"
        "    et.type = 'hidden';"
        "    et.name = '__EVENTTARGET';"
        "    et.value = 'csvdownload';"
        "    f.appendChild(et);"
        "    f.submit();"
        "  }"
        "}"
    )


def main() -> int:
    args = parse_args()
    zip_codes = load_zip_codes(args.zips)
    if not zip_codes:
        print("No zip codes found.")
        return 1

    download_dir = pathlib.Path(args.download_dir).expanduser()
    download_dir.mkdir(parents=True, exist_ok=True)

    driver = setup_driver(str(download_dir), args.headless)
    wait = WebDriverWait(driver, args.timeout)

    try:
        for idx, zip_code in enumerate(zip_codes, start=1):
            print(f"[{idx}/{len(zip_codes)}] Zip {zip_code}")
            url = build_url_with_zip(args.base_url, zip_code)
            driver.get(url)

            # Wait for results table to render.
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.LicTable1")))

            start_time = time.time()
            trigger_csv_export(driver)

            downloaded = wait_for_download(str(download_dir), start_time, args.timeout)
            target_name = f"mn_ccap_raw_{zip_code}{downloaded.suffix}"
            target_path = download_dir / target_name
            if downloaded != target_path:
                try:
                    downloaded.rename(target_path)
                except OSError:
                    # If rename fails (e.g., same filesystem restrictions), leave as-is.
                    target_path = downloaded
            print(f"Downloaded to {target_path}")

            if args.sleep > 0 and idx < len(zip_codes):
                time.sleep(args.sleep)
    finally:
        driver.quit()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
