import argparse, csv
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]


def classify_error(error: str) -> str:
    e = (error or "").strip().lower()
    if not e:
        return "unknown"

    if e.startswith("no_favicon_found"):
        return "no_favicon_found"

    if "favicon_http_status:" in e:
        try:
            code = int(e.split("favicon_http_status:", 1)[1].split()[0].strip().rstrip(";"))
        except Exception:
            return "favicon_http_status_other"
        if code == 403:
            return "favicon_http_403_forbidden"
        if code == 404:
            return "favicon_http_404_not_found"
        if code == 429:
            return "favicon_http_429_rate_limited"
        if 500 <= code <= 599:
            return "favicon_http_5xx_server_error"
        if 400 <= code <= 499:
            return "favicon_http_4xx_other"
        if 300 <= code <= 399:
            return "favicon_http_3xx_redirect"
        return "favicon_http_status_other"

    if "homepage_http_status:" in e:
        try:
            code = int(e.split("homepage_http_status:", 1)[1].split()[0].strip().rstrip(";"))
        except Exception:
            return "homepage_http_status_other"
        if code == 403:
            return "homepage_http_403_forbidden"
        if code == 404:
            return "homepage_http_404_not_found"
        if code == 429:
            return "homepage_http_429_rate_limited"
        if 500 <= code <= 599:
            return "homepage_http_5xx_server_error"
        if 400 <= code <= 499:
            return "homepage_http_4xx_other"
        if 300 <= code <= 399:
            return "homepage_http_3xx_redirect"
        return "homepage_http_status_other"

    if "err_name_not_resolved" in e:
        return "dns_unresolved"
    if "err_cert_" in e or "unable to verify the first certificate" in e:
        return "tls_cert_error"
    if "err_connection" in e or "connection refused" in e:
        return "connection_error"
    if e.startswith("timeout:"):
        return "navigation_timeout"
    if "execution context was destroyed" in e:
        return "navigation_race"
    if e.startswith("favicon_detect_error:"):
        return "favicon_detect_error"
    if e.startswith("download_error:"):
        return "favicon_download_error"

    if "http_status:" in e:
        try:
            code = int(e.split("http_status:", 1)[1].split()[0].strip())
        except Exception:
            return "http_status_other"
        if code == 403:
            return "http_403_forbidden"
        if code == 404:
            return "http_404_not_found"
        if code == 429:
            return "http_429_rate_limited"
        if 500 <= code <= 599:
            return "http_5xx_server_error"
        if 400 <= code <= 499:
            return "http_4xx_other"
        if 300 <= code <= 399:
            return "http_3xx_redirect"
        return "http_status_other"

    if e.startswith("nav_error:"):
        return "navigation_error_other"
    if e.startswith("unexpected:"):
        return "unexpected_error"

    return "other"


def summarize(results_csv: Path, show_examples: int = 3):
    with results_csv.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    status_counts = Counter((r.get("status") or "").strip() for r in rows)

    failures = [r for r in rows if (r.get("status") or "").strip() == "-1"]
    failure_categories = Counter(classify_error(r.get("error", "")) for r in failures)

    cat_examples = {}
    for r in failures:
        cat = classify_error(r.get("error", ""))
        cat_examples.setdefault(cat, [])
        if len(cat_examples[cat]) < show_examples:
            cat_examples[cat].append((r.get("site", ""), r.get("error", "")))

    print(f"Input: {results_csv}")
    print(f"Total rows: {total}")
    print("Status counts:")
    for s in ["2", "-1", "0", "1"]:
        if s in status_counts:
            print(f"  status {s}: {status_counts[s]}")

    print("\nFailure categories:")
    if not failures:
        print("  none")
        return

    for cat, n in failure_categories.most_common():
        pct = (n / len(failures)) * 100
        print(f"  {cat}: {n} ({pct:.1f}%)")
        for site, err in cat_examples.get(cat, []):
            print(f"    - {site}: {err[:160]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Summarize favicon crawl results by failure category")
    ap.add_argument(
        "--input",
        default=str(REPO_ROOT / "data" / "favicons" / "D2_ALGERIA" / "results.csv"),
        help="Path to results.csv",
    )
    ap.add_argument("--examples", type=int, default=2, help="Examples per category")
    args = ap.parse_args()

    path = Path(args.input).resolve()
    if not path.exists():
        raise SystemExit(f"results.csv not found: {path}")

    summarize(path, show_examples=max(0, args.examples))
