"""
Generates summary_crawl_favicons.csv under results/.

Columns:
    category, n_sites, pct_success, pct_failed,
    pct_http_4xx, pct_http_5xx, pct_rate_limited, pct_dns,
    pct_timeout, pct_connection, pct_nav_or_download, pct_no_favicon, pct_other
"""

import csv, sys
from collections import Counter
from pathlib import Path

csv.field_size_limit(sys.maxsize)

VALIDATE_D2_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[4]
FAVICONS_DIR = REPO_ROOT / "data" / "favicons"
OUTPUT_CSV = VALIDATE_D2_DIR / "results" / "summary_crawl_favicons.csv"

ERROR_CATEGORIES = [
    "favicon_http_403_forbidden",
    "favicon_http_404_not_found",
    "dns_unresolved",
    "navigation_timeout",
    "navigation_error_other",
    "favicon_http_5xx_server_error",
    "favicon_http_4xx_other",
    "favicon_download_error",
    "connection_error",
    "favicon_http_429_rate_limited",
    "no_favicon_found",
    "other",
]

GROUPED_ERROR_COLUMNS = [
    "pct_http_4xx",
    "pct_http_5xx",
    "pct_rate_limited",
    "pct_dns",
    "pct_timeout",
    "pct_connection",
    "pct_nav_or_download",
    "pct_no_favicon",
    "pct_other",
]

COLLAPSED_COLUMNS = [
    "pct_connection",
    "pct_nav_or_download",
    "pct_no_favicon",
    "pct_other",
    "pct_rate_limited",
    "pct_http_5xx",
]


def classify_error(error: str) -> str:
    e = (error or "").strip().lower()
    if not e:
        return "other"

    if e.startswith("no_favicon_found"):
        return "no_favicon_found"

    if "favicon_http_status:" in e:
        try:
            code = int(e.split("favicon_http_status:", 1)[1].split()[0].strip().rstrip(";"))
        except Exception:
            return "other"
        if code == 403: return "favicon_http_403_forbidden"
        if code == 404: return "favicon_http_404_not_found"
        if code == 429: return "favicon_http_429_rate_limited"
        if 500 <= code <= 599: return "favicon_http_5xx_server_error"
        if 400 <= code <= 499: return "favicon_http_4xx_other"
        return "other"

    if "homepage_http_status:" in e:
        return "other"

    if "err_name_not_resolved" in e:
        return "dns_unresolved"
    if "err_cert_" in e or "unable to verify the first certificate" in e:
        return "other"
    if "err_connection" in e or "connection refused" in e or "connection_reset" in e:
        return "connection_error"
    if e.startswith("timeout:"):
        return "navigation_timeout"
    if "execution context was destroyed" in e:
        return "navigation_error_other"
    if e.startswith("favicon_detect_error:"):
        return "favicon_download_error"
    if e.startswith("download_error:"):
        return "favicon_download_error"
    if e.startswith("nav_error:"):
        return "navigation_error_other"

    return "other"


def process_folder(folder: Path) -> dict | None:
    results_csv = folder / "results.csv"
    if not results_csv.exists():
        return None

    with results_csv.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return None

    n = len(rows)
    n_success = sum(1 for r in rows if (r.get("status") or "").strip() == "2")
    failures = [r for r in rows if (r.get("status") or "").strip() == "-1"]

    error_counts = Counter(classify_error(r.get("error", "")) for r in failures)

    row_out = {
        "category": folder.name,
        "n_sites": n,
        "pct_success": round(100 * n_success / n, 1) if n else 0,
        "pct_failed": round(100 * len(failures) / n, 1) if n else 0,
    }

    grouped_counts = {
        "pct_http_4xx": (
            error_counts.get("favicon_http_403_forbidden", 0)
            + error_counts.get("favicon_http_404_not_found", 0)
            + error_counts.get("favicon_http_4xx_other", 0)
        ),
        "pct_http_5xx": error_counts.get("favicon_http_5xx_server_error", 0),
        "pct_rate_limited": error_counts.get("favicon_http_429_rate_limited", 0),
        "pct_dns": error_counts.get("dns_unresolved", 0),
        "pct_timeout": error_counts.get("navigation_timeout", 0),
        "pct_connection": error_counts.get("connection_error", 0),
        "pct_nav_or_download": (
            error_counts.get("navigation_error_other", 0)
            + error_counts.get("favicon_download_error", 0)
        ),
        "pct_no_favicon": error_counts.get("no_favicon_found", 0),
        "pct_other": error_counts.get("other", 0),
    }

    grouped_counts["pct_other_grouped"] = sum(grouped_counts[c] for c in COLLAPSED_COLUMNS)

    for col in GROUPED_ERROR_COLUMNS:
        if col in COLLAPSED_COLUMNS:
            continue
        row_out[col] = round(100 * grouped_counts[col] / n, 1) if n else 0

    row_out["pct_other_grouped"] = round(100 * grouped_counts["pct_other_grouped"] / n, 1) if n else 0

    return row_out


def main():
    folders = sorted(
        f for f in FAVICONS_DIR.iterdir()
        if f.is_dir() and (f / "results.csv").exists()
    )

    if not folders:
        raise SystemExit(f"No results.csv found under {FAVICONS_DIR}")

    fieldnames = [
        "category",
        "n_sites",
        "pct_success",
        "pct_failed",
        "pct_http_4xx",
        "pct_dns",
        "pct_timeout",
        "pct_other_grouped",
    ]

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for folder in folders:
            row = process_folder(folder)
            if row:
                w.writerow(row)
                print(f"  {folder.name}: {row['n_sites']} sites, {row['pct_success']}% success")

    print(f"\nSaved: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
