"""
generate_sitemaps.py

Generate sitemap XML files with 50MB max size per file and a sitemap index.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable, List


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sitemap XML files.")
    parser.add_argument(
        "--root",
        default=str(ROOT),
        help="Root directory to scan for HTML files",
    )
    parser.add_argument(
        "--base-url",
        default="https://civicdatadog.com",
        help="Base URL for sitemap entries",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=50_000_000,
        help="Max bytes per sitemap file (default: 50MB)",
    )
    return parser.parse_args()


def should_skip(path: Path) -> bool:
    parts = {p.lower() for p in path.parts}
    if ".git" in parts or ".venv" in parts:
        return True
    if "python-scripts" in parts:
        return True
    return False


def iter_html_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        if path.suffix.lower() not in {".html", ".htm"}:
            continue
        if path.name.startswith("sitemap"):
            continue
        if should_skip(path):
            continue
        yield path


def to_url(base_url: str, root: Path, path: Path) -> str:
    rel = path.relative_to(root).as_posix()
    if path.name in {"index.html", "index.htm"}:
        parent = path.parent.relative_to(root).as_posix()
        if parent == ".":
            return f"{base_url}/"
        return f"{base_url}/{parent}/"
    return f"{base_url}/{rel}"


def url_entry(loc: str) -> str:
    return f"  <url>\n    <loc>{loc}</loc>\n  </url>\n"


def write_sitemap(path: Path, urls: List[str]) -> None:
    header = '<?xml version="1.0" encoding="UTF-8"?>\n' \
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    footer = "</urlset>\n"
    body = "".join(url_entry(url) for url in urls)
    path.write_text(header + body + footer, encoding="utf-8")


def write_index(path: Path, base_url: str, sitemap_files: List[Path]) -> None:
    header = '<?xml version="1.0" encoding="UTF-8"?>\n' \
             '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    footer = "</sitemapindex>\n"
    items = []
    for file_path in sitemap_files:
        loc = f"{base_url}/{file_path.name}"
        items.append(f"  <sitemap>\n    <loc>{loc}</loc>\n  </sitemap>\n")
    path.write_text(header + "".join(items) + footer, encoding="utf-8")


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    base_url = args.base_url.rstrip("/")

    urls = sorted({to_url(base_url, root, path) for path in iter_html_files(root)})
    if not urls:
        print("No HTML files found.")
        return 1

    sitemap_files: List[Path] = []
    current: List[str] = []
    current_bytes = 0
    for url in urls:
        entry = url_entry(url)
        if current and (current_bytes + len(entry.encode("utf-8")) > args.max_bytes or len(current) >= 50_000):
            sitemap_path = root / f"sitemap-{len(sitemap_files) + 1}.xml"
            write_sitemap(sitemap_path, current)
            sitemap_files.append(sitemap_path)
            current = []
            current_bytes = 0
        current.append(url)
        current_bytes += len(entry.encode("utf-8"))

    if current:
        sitemap_path = root / f"sitemap-{len(sitemap_files) + 1}.xml"
        write_sitemap(sitemap_path, current)
        sitemap_files.append(sitemap_path)

    index_path = root / "sitemap.xml"
    write_index(index_path, base_url, sitemap_files)
    print(f"Wrote {len(sitemap_files)} sitemap files and index to {index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
