import argparse
import ast
import json
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parents[4]
DATA_DIR = REPO_ROOT / "data" / "TLS_certificates"
DEFAULT_MAPPING = REPO_ROOT / "site_catalogs_construction" / "catalogs" / "D2_to_global"

COUNTRY_FILE_BY_CODE = {
    "dz": "algeria_urls_data.json",
    "de": "germany_urls_data.json",
    "in": "india_urls_data.json",
    "ae": "uae_urls_data.json",
    "us": "usa_urls_data.json",
}

OTHER_COUNTRY_FILES = [
    "algeria_urls_data.json",
    "germany_urls_data.json",
    "india_urls_data.json",
    "uae_urls_data.json",
    "usa_urls_data.json",
]


def normalize_host(value):
    text = (value or "").strip().lower()
    text = text.removeprefix("http://").removeprefix("https://")
    text = text.split("/", 1)[0].split(":", 1)[0].strip(".")
    if text.startswith("www."):
        return text[4:]
    return text


def host_candidates(host):
    h = normalize_host(host)
    if not h:
        return []
    return [h, "www." + h]


def parse_url_host(url_text):
    raw = (url_text or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    return normalize_host(urlparse(raw).netloc)


def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def san_matches_host(san, host):
    s = (san or "").strip().lower()
    h = normalize_host(host)
    if not s or not h:
        return False

    if s.startswith("*."):
        base = s[2:]
        return h.endswith("." + base)
    return normalize_host(s) == h


def parse_tls_field(tls_value):
    if not tls_value or not isinstance(tls_value, list) or len(tls_value) < 2:
        return set(), ""
    sans_raw = tls_value[0] if isinstance(tls_value[0], list) else []
    org_raw = tls_value[1] if isinstance(tls_value[1], str) else ""
    sans = {str(x).strip().lower() for x in sans_raw if str(x).strip()}
    org = org_raw.strip().lower()
    return sans, org


def build_tls_index(data_dict):
    out = {}
    for url_text, attrs in data_dict.items():
        host = parse_url_host(url_text)
        if not host:
            continue
        tls_value = attrs.get("tls") if isinstance(attrs, dict) else None
        sans, org = parse_tls_field(tls_value)
        out[host] = (sans, org)
    return out


def get_tls_for_host(index, host):
    for cand in host_candidates(host):
        hit = index.get(cand)
        if hit:
            return hit
    return None


def load_mapping_equivalents(mapping_csv, country, d2_site):
    import csv

    country = normalize_host(country)
    d2_site = normalize_host(d2_site)

    if mapping_csv.is_dir():
        country_file = mapping_csv / f"d2_to_global_{country.upper()}.csv"
        if not country_file.exists():
            return []

        out = []
        with country_file.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            required = {"d2_site", "global_site"}
            if not required.issubset(set(reader.fieldnames or [])):
                return []
            for row in reader:
                if normalize_host(row.get("d2_site", "")) != d2_site:
                    continue
                eq = normalize_host(row.get("global_site", ""))
                if eq and eq not in out:
                    out.append(eq)
        return out

    with mapping_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])

        pair_required = {"d2_site", "global_site"}
        if pair_required.issubset(fieldnames):
            out = []
            for row in reader:
                if normalize_host(row.get("d2_site", "")) != d2_site:
                    continue
                eq = normalize_host(row.get("global_site", ""))
                if eq and eq not in out:
                    out.append(eq)
            return out

        for row in reader:
            if normalize_host(row.get("country", "")) != country:
                continue
            if normalize_host(row.get("D2_site", "")) != d2_site:
                continue
            raw = (row.get("equivalents") or "").strip()
            if not raw:
                return []
            try:
                vals = ast.literal_eval(raw)
            except Exception:
                vals = [x.strip() for x in raw.split(",") if x.strip()]
            return [normalize_host(str(x)) for x in vals if normalize_host(str(x))]
    return []


def resolve_equivalent_tls(eq_host, global_index, country_indexes):
    tls = get_tls_for_host(global_index, eq_host)
    if tls:
        return tls, "D1"

    for code, idx in country_indexes.items():
        tls = get_tls_for_host(idx, eq_host)
        if tls:
            return tls, "equivalents_not_in_D1", code

    return None, "no_tls_data"


def validate_pair(d2_host, eq_host, d2_tls, eq_tls):
    d2_sans, d2_org = d2_tls
    eq_sans, eq_org = eq_tls

    reasons = []

    if d2_sans and eq_sans:
        if any(san_matches_host(s, eq_host) for s in d2_sans):
            reasons.append("equivalent in D2 SAN")
        if any(san_matches_host(s, d2_host) for s in eq_sans):
            reasons.append("D2 in equivalent SAN")
        if d2_sans == eq_sans:
            reasons.append("identical SAN set")

    if d2_org and eq_org and d2_org == eq_org:
        reasons.append("same organizationName")

    return len(reasons) > 0, reasons


def main():
    parser = argparse.ArgumentParser(
        description="Validate D2 site-equivalent links using cached TLS fields from *_urls_data.json files."
    )
    parser.add_argument("--country", required=True, help="Country code for D2 site (dz,de,in,ae,us)")
    parser.add_argument("--d2-site", required=True, help="D2 site host or URL")
    parser.add_argument(
        "--equivalent",
        action="append",
        default=[],
        help="Equivalent host/URL; pass multiple times. If omitted, values come from mapping CSV.",
    )
    parser.add_argument(
        "--mapping-csv",
        default=str(DEFAULT_MAPPING),
        help="Path to legacy D2_to_GLOBAL.csv or directory with d2_to_global_*.csv",
    )
    parser.add_argument("--output-json", default="", help="Optional path to save full JSON output")
    args = parser.parse_args()

    country = normalize_host(args.country)
    d2_host = normalize_host(args.d2_site)
    if country not in COUNTRY_FILE_BY_CODE:
        raise SystemExit(f"Unsupported country '{country}'. Expected one of: {sorted(COUNTRY_FILE_BY_CODE)}")

    mapping_csv = Path(args.mapping_csv).resolve()

    equivalents = [normalize_host(x) for x in args.equivalent if normalize_host(x)]
    if not equivalents:
        equivalents = load_mapping_equivalents(mapping_csv, country, d2_host)

    if not equivalents:
        raise SystemExit("No equivalents found. Pass --equivalent or ensure mapping CSV has this D2 site.")

    global_data = load_json(DATA_DIR / "global_urls_data.json")
    global_index = build_tls_index(global_data)

    d2_file = DATA_DIR / COUNTRY_FILE_BY_CODE[country]
    d2_data = load_json(d2_file)
    d2_index = build_tls_index(d2_data)

    d2_tls = get_tls_for_host(d2_index, d2_host)
    if not d2_tls:
        out = {
            "country": country,
            "d2_site": d2_host,
            "status": "no_d2_tls_data",
            "equivalents_checked": len(equivalents),
            "results": [],
            "validated_any": False,
        }
        print(json.dumps(out, indent=2))
        return

    country_indexes = {}
    for file_name in OTHER_COUNTRY_FILES:
        code = next((k for k, v in COUNTRY_FILE_BY_CODE.items() if v == file_name), None)
        if not code:
            continue
        country_indexes[code] = build_tls_index(load_json(DATA_DIR / file_name))

    results = []
    for eq in equivalents:
        resolved = resolve_equivalent_tls(eq, global_index, country_indexes)

        if resolved[1] == "no_tls_data":
            results.append(
                {
                    "equivalent": eq,
                    "pool": "no_tls_data",
                    "validated": False,
                    "status": "missing_equivalent_tls",
                    "reasons": [],
                }
            )
            continue

        eq_tls, pool, *rest = resolved
        src_country = rest[0] if rest else None

        validated, reasons = validate_pair(d2_host, eq, d2_tls, eq_tls)
        row = {
            "equivalent": eq,
            "pool": pool,
            "validated": validated,
            "status": "ok",
            "reasons": reasons,
        }
        if src_country:
            row["equivalent_country_source"] = src_country
        results.append(row)

    validated_any = any(r["validated"] for r in results)
    out = {
        "country": country,
        "d2_site": d2_host,
        "status": "ok",
        "equivalents_checked": len(results),
        "validated_any": validated_any,
        "validated_count": sum(1 for r in results if r["validated"]),
        "results": results,
    }

    print(json.dumps(out, indent=2))

    if args.output_json:
        out_path = Path(args.output_json).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)


if __name__ == "__main__":
    main()
