"""
generate_provider_pages.py

Generate static HTML pages for Minnesota CCAP providers using the site's
existing HTML structure.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "python-scripts" / "data"


def parse_args() -> argparse.Namespace:
    default_input = DATA_DIR / "mn_ccap_providers_all.csv"
    if not default_input.exists():
        default_input = DATA_DIR / "mn_ccap_providers.csv"
    parser = argparse.ArgumentParser(description="Generate provider HTML pages.")
    parser.add_argument(
        "--input",
        default=str(default_input),
        help="CSV input of providers",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "providers" / "mn"),
        help="Directory to write provider pages",
    )
    parser.add_argument(
        "--index-path",
        default=str(ROOT / "providers" / "mn" / "index.html"),
        help="Path to write the provider index page",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Providers per index page (default: 100)",
    )
    return parser.parse_args()


def read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def provider_slug(row: Dict[str, str]) -> str:
    name = row.get("provider_name", "") or "provider"
    license_number = row.get("license_number", "")
    base = slugify(name)
    if license_number:
        base = f"{base}-{slugify(license_number)}"
    return base or "provider"


def unique_slugs(rows: Iterable[Dict[str, str]]) -> Dict[int, str]:
    seen = {}
    slugs = {}
    for idx, row in enumerate(rows):
        base = provider_slug(row)
        if base not in seen:
            seen[base] = 1
            slugs[idx] = base
            continue
        seen[base] += 1
        slugs[idx] = f"{base}-{seen[base]}"
    return slugs


def html_header(title: str, description: str, canonical: str, prefix: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <meta name="description" content="{description}">
    <meta name="author" content="Civic Datadog">
    <meta name="robots" content="index, follow">
    <link rel="canonical" href="{canonical}">

    <!-- Open Graph / Facebook -->
    <meta property="og:type" content="website">
    <meta property="og:url" content="{canonical}">
    <meta property="og:title" content="{title}">
    <meta property="og:description" content="{description}">
    <meta property="og:site_name" content="Civic Datadog">

    <!-- Twitter -->
    <meta name="twitter:card" content="summary">
    <meta name="twitter:url" content="{canonical}">
    <meta name="twitter:title" content="{title}">
    <meta name="twitter:description" content="{description}">
    <meta name="twitter:site" content="@civicdatadog">

    <link rel="stylesheet" href="{prefix}style.css">

    <!-- Google tag (gtag.js) -->
    <script async src="https://www.googletagmanager.com/gtag/js?id=G-WKGC1TET64"></script>
    <script>
      window.dataLayer = window.dataLayer || [];
      function gtag(){{dataLayer.push(arguments);}}
      gtag('js', new Date());
      gtag('config', 'G-WKGC1TET64');
    </script>
  </head>
  <body>
    <header>
      <div class="wrapper">
        <div class="brand">
          <div class="brand-block">
            <div class="logo" aria-hidden="true">CD</div>
            <div>
              <h1>Minnesota Child Care Providers</h1>
              <p>CCAP provider details for Minnesota.</p>
            </div>
          </div>
          <p class="small"><a href="https://civicdatadog.com">civicdatadog.com</a> | X: <a href="https://x.com/civicdatadog">https://x.com/civicdatadog</a></p>
        </div>
        <nav aria-label="Primary">
          <ul>
            <li><a href="{prefix}index.html">Home</a></li>
            <li><a href="{prefix}spending/index.html">Spending</a></li>
            <li><a href="{prefix}states/index.html">States</a></li>
            <li><a href="{prefix}minnesota.html">Minnesota</a></li>
            <li><a href="{prefix}sources.html">Sources</a></li>
            <li><a href="{prefix}methodology.html">Methodology</a></li>
          </ul>
        </nav>
      </div>
    </header>
"""


def html_footer() -> str:
    return """    <footer>
      <div class="wrapper">
        <p>Powered by <a href="https://athena.live">Athena.live</a> — public data aggregation &amp; AI analysis</p>
      </div>
    </footer>
  </body>
</html>
"""


def build_provider_page(row: Dict[str, str], canonical: str, prefix: str) -> str:
    name = row.get("provider_name", "Provider").strip() or "Provider"
    description = f"Details for {name} in Minnesota."
    title = f"{name} - Minnesota Child Care Provider | Civic Datadog"
    address = row.get("address", "")
    city = row.get("city", "")
    state = row.get("state", "")
    zip_code = row.get("zip", "")
    license_number = row.get("license_number", "")
    license_type = row.get("license_type", "")
    status = row.get("status", "")
    county = row.get("county", "")
    website = row.get("places_website", "")
    phone = row.get("places_phone", "")
    maps_query = ", ".join([p for p in [address, city, state, zip_code] if p])
    maps_link = f"https://www.google.com/maps/search/?api=1&query={maps_query.replace(' ', '+')}" if maps_query else ""

    details = [
        ("License number", license_number),
        ("License type", license_type),
        ("Status", status),
        ("Address", address),
        ("City", city),
        ("State", state),
        ("ZIP", zip_code),
        ("County", county),
        ("Phone", phone),
        ("Website", website),
    ]

    detail_rows = "\n".join(
        f"          <tr><th scope=\"row\">{label}</th><td>{value}</td></tr>"
        for label, value in details
        if value
    )

    maps_row = (
        f"          <tr><th scope=\"row\">Map</th><td><a href=\"{maps_link}\">View on Google Maps</a></td></tr>"
        if maps_link
        else ""
    )

    return (
        html_header(title, description, canonical, prefix)
        + f"""    <main class="wrapper">
      <section class="card">
        <h2>{name}</h2>
        <p>Licensed Minnesota child care provider listing compiled from public records.</p>
      </section>

      <section>
        <h2>Provider Details</h2>
        <div class="card">
          <div class="table-wrap" role="region" aria-label="Provider details">
            <table>
              <tbody>
{detail_rows}
{maps_row}
              </tbody>
            </table>
          </div>
          <p class="small">Back to <a href="{prefix}minnesota.html">Minnesota</a> or the <a href="{prefix}providers/mn/index.html">provider directory</a>.</p>
        </div>
      </section>
    </main>
"""
        + html_footer()
    )


def pagination_nav(page_num: int, total_pages: int, prefix: str) -> str:
    if total_pages <= 1:
        return ""
    prev_link = ""
    next_link = ""
    if page_num > 1:
        prev_href = f"{prefix}providers/mn/page/{page_num - 1}.html"
        prev_link = f'<a href="{prev_href}">Previous</a>'
    if page_num < total_pages:
        next_href = f"{prefix}providers/mn/page/{page_num + 1}.html"
        next_link = f'<a href="{next_href}">Next</a>'
    parts = [p for p in [prev_link, next_link] if p]
    if not parts:
        return ""
    return f"<p class=\"small\">{' | '.join(parts)}</p>"


def build_index_page(
    rows: List[Dict[str, str]],
    slugs: List[str],
    prefix: str,
    page_num: int,
    total_pages: int,
    canonical: str,
) -> str:
    title = "Minnesota Child Care Providers Directory | Civic Datadog"
    description = "Directory of Minnesota child care providers (CCAP) with detail pages."

    items = []
    for idx, row in enumerate(rows):
        name = row.get("provider_name", "Provider").strip() or "Provider"
        city = row.get("city", "")
        zip_code = row.get("zip", "")
        meta = ", ".join([p for p in [city, zip_code] if p])
        label = f"{name} ({meta})" if meta else name
        items.append(f"            <li><a href=\"{slugs[idx]}.html\">{label}</a></li>")

    items_html = "\n".join(items)
    nav_html = pagination_nav(page_num, total_pages, prefix)
    return (
        html_header(title, description, canonical, prefix)
        + f"""    <main class="wrapper">
      <section class="card">
        <h2>Minnesota Child Care Providers</h2>
        <p>Directory of licensed Minnesota child care providers compiled from public records.</p>
      </section>

      <section>
        <h2>Provider Directory (Page {page_num} of {total_pages})</h2>
        <div class="card">
          <ul>
{items_html}
          </ul>
          {nav_html}
          <p class="small">Back to <a href="{prefix}minnesota.html">Minnesota</a> spending overview.</p>
        </div>
      </section>
    </main>
"""
        + html_footer()
    )


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        return 1

    rows = read_rows(input_path)
    if not rows:
        print("ERROR: No rows found in input.")
        return 1

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = Path(args.index_path)
    index_path.parent.mkdir(parents=True, exist_ok=True)

    slugs = unique_slugs(rows)
    prefix = "../../"

    for idx, row in enumerate(rows):
        slug = slugs[idx]
        canonical = f"https://civicdatadog.com/providers/mn/{slug}.html"
        html_text = build_provider_page(row, canonical, prefix)
        (output_dir / f"{slug}.html").write_text(html_text, encoding="utf-8")

    per_page = max(args.per_page, 1)
    total_pages = (len(rows) + per_page - 1) // per_page
    page_dir = output_dir / "page"
    page_dir.mkdir(parents=True, exist_ok=True)

    for page_num in range(1, total_pages + 1):
        start = (page_num - 1) * per_page
        end = start + per_page
        page_rows = rows[start:end]
        page_slugs = [slugs[idx] for idx in range(start, min(end, len(rows)))]
        page_prefix = "../../../"
        canonical = f"https://civicdatadog.com/providers/mn/page/{page_num}.html"
        page_html = build_index_page(
            page_rows,
            page_slugs,
            page_prefix,
            page_num,
            total_pages,
            canonical,
        )
        (page_dir / f"{page_num}.html").write_text(page_html, encoding="utf-8")

    index_html = build_index_page(
        rows[:per_page],
        [slugs[idx] for idx in range(min(per_page, len(rows)))],
        prefix,
        1,
        total_pages,
        "https://civicdatadog.com/providers/mn/index.html",
    )
    index_path.write_text(index_html, encoding="utf-8")
    print(f"Wrote {len(rows)} provider pages to {output_dir}")
    print(f"Wrote index to {index_path}")
    print(f"Wrote {total_pages} paginated index pages to {page_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
