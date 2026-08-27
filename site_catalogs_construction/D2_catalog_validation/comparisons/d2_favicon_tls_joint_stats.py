import ast
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from tqdm import tqdm

from compare_favicon_embedding import favicon_similarity_embedding

_THIS_DIR = Path(__file__).resolve().parent
_VALIDATE_D2_DIR = _THIS_DIR.parent
_REPO_ROOT = _VALIDATE_D2_DIR.parent.parent

MAPPING_CSV = _REPO_ROOT / "site_catalogs_construction" / "catalogs" / "D2_to_global"
FAVICONS_ROOT = _REPO_ROOT / "data" / "favicons"
CERTIFICATES_DIR = _REPO_ROOT / "data" / "TLS_certificates"
OUTPUT_DIR = _VALIDATE_D2_DIR / "results"
THRESHOLD = 0.85

D2_FOLDER_BY_COUNTRY_CODE = {
    "dz": "D2_ALGERIA",
    "in": "D2_INDIA",
    "ae": "D2_UAE",
    "de": "D2_GERMANY",
    "us": "D2_USA",
}

COUNTRY_ALIASES_TO_CODE = {
    "algeria": "dz",
    "india": "in",
    "uae": "ae",
    "united arab emirates": "ae",
    "germany": "de",
    "usa": "us",
    "united states": "us",
    "united states of america": "us",
}

CERT_FILE_BY_COUNTRY_CODE = {
    "dz": "algeria_urls_data.json",
    "de": "germany_urls_data.json",
    "in": "india_urls_data.json",
    "ae": "uae_urls_data.json",
    "us": "usa_urls_data.json",
}

VALID_ICON_EXTS = {".ico", ".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".avif"}


@dataclass(frozen=True)
class MappingRow:
    country: str
    d2_site: str
    equivalents_raw: str
    equivalents: tuple


def normalize_host(value):
    host = (value or "").strip().lower()
    host = host.removeprefix("http://").removeprefix("https://")
    host = host.split("/", 1)[0].split(":", 1)[0].strip(".")
    return host


def host_candidates(host):
    h = normalize_host(host)
    if not h:
        return []
    if h.startswith("www."):
        return [h, h[4:]]
    return [h, "www." + h]


def parse_equivalents(raw):
    text = (raw or "").strip()
    if not text:
        return tuple()

    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple)):
            vals = [normalize_host(str(x)) for x in parsed if str(x).strip()]
            return tuple(v for v in vals if v)
    except Exception:
        pass

    vals = [normalize_host(x) for x in text.split(",") if x.strip()]
    return tuple(v for v in vals if v)


def _country_from_d2_to_global_name(path):
    stem = path.stem.lower()
    prefix = "d2_to_global_"
    if stem.startswith(prefix):
        return country_to_code(stem[len(prefix):])
    return ""


def _rows_from_country_pair_csv(csv_path, country_code):
    grouped = {}

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"d2_site", "global_site"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"Mapping CSV missing required columns {required}: {csv_path}")

        for row in reader:
            d2_site = normalize_host(row.get("d2_site", ""))
            global_site = normalize_host(row.get("global_site", ""))
            if not d2_site or not global_site:
                continue
            grouped.setdefault(d2_site, [])
            if global_site not in grouped[d2_site]:
                grouped[d2_site].append(global_site)

    rows = []
    for d2_site, equivalents in grouped.items():
        rows.append(
            MappingRow(
                country=country_code,
                d2_site=d2_site,
                equivalents_raw=str(equivalents),
                equivalents=tuple(equivalents),
            )
        )
    return rows


def read_mapping_rows(mapping_source):
    if mapping_source.is_dir():
        rows = []
        for csv_path in sorted(mapping_source.glob("d2_to_global_*.csv")):
            country_code = _country_from_d2_to_global_name(csv_path)
            if not country_code:
                continue
            rows.extend(_rows_from_country_pair_csv(csv_path, country_code))
        return rows

    with mapping_source.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fields = set(reader.fieldnames or [])

        legacy_required = {"country", "D2_site", "equivalents"}
        pair_required = {"d2_site", "global_site"}

        if legacy_required.issubset(fields):
            rows = []
            for row in reader:
                country = normalize_host(row.get("country", ""))
                d2_site = normalize_host(row.get("D2_site", ""))
                equivalents_raw = (row.get("equivalents") or "").strip()
                if not country or not d2_site:
                    continue
                rows.append(
                    MappingRow(
                        country=country,
                        d2_site=d2_site,
                        equivalents_raw=equivalents_raw,
                        equivalents=parse_equivalents(equivalents_raw),
                    )
                )
            return rows

    if pair_required.issubset(fields):
        country_code = _country_from_d2_to_global_name(mapping_source)
        if not country_code:
            raise ValueError(
                f"Cannot infer country from filename: {mapping_source.name}. "
                "Expected d2_to_global_<COUNTRY>.csv"
            )
        return _rows_from_country_pair_csv(mapping_source, country_code)

    raise ValueError(
        f"Unsupported mapping format: {mapping_source}. "
        "Expected legacy D2_to_GLOBAL.csv columns or per-country d2_to_global_*.csv"
    )


def country_to_code(country_value):
    c = normalize_host(country_value)
    if len(c) == 2:
        return c
    return COUNTRY_ALIASES_TO_CODE.get(c, c)


def infer_d2_folder_for_country(code, favicons_root):
    if code in D2_FOLDER_BY_COUNTRY_CODE:
        return D2_FOLDER_BY_COUNTRY_CODE[code]
    direct = f"D2_{code.upper()}"
    if (favicons_root / direct / "icons").exists():
        return direct
    return None


def build_icon_index(icon_dir):
    index = {}
    if not icon_dir.exists():
        return index

    for path in sorted(icon_dir.iterdir()):
        if not path.is_file() or path.suffix.lower() not in VALID_ICON_EXTS:
            continue
        stem = normalize_host(path.stem)
        if stem and stem not in index:
            index[stem] = str(path.resolve())
    return index


def lookup_icon(index, site):
    for cand in host_candidates(site):
        hit = index.get(cand)
        if hit:
            return hit
    return None


def resolve_equivalent_icon(equivalent, d1_index, d2_eq_index):
    hit = lookup_icon(d1_index, equivalent)
    if hit:
        return hit, "D1"

    hit = lookup_icon(d2_eq_index, equivalent)
    if hit:
        return hit, "D2_equivalents_not_in_D1"

    return None, None


def parse_tls_field(tls_value):
    if not tls_value or not isinstance(tls_value, list) or len(tls_value) < 2:
        return set(), ""

    sans_raw = tls_value[0] if isinstance(tls_value[0], list) else []
    org_raw = tls_value[1] if isinstance(tls_value[1], str) else ""

    sans = {str(x).strip().lower() for x in sans_raw if str(x).strip()}
    org = org_raw.strip().lower()
    return sans, org


def extract_url_host(url_text):
    raw = (url_text or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    return normalize_host(urlparse(raw).netloc)


def build_tls_index(data_dict):
    out = {}
    for url_text, attrs in data_dict.items():
        host = extract_url_host(url_text)
        if not host:
            continue
        tls_value = attrs.get("tls") if isinstance(attrs, dict) else None
        out[host] = parse_tls_field(tls_value)
    return out


def lookup_tls(index, site):
    for cand in host_candidates(site):
        hit = index.get(cand)
        if hit:
            return hit
    return None


def san_matches_host(san, host):
    s = (san or "").strip().lower()
    h = normalize_host(host)
    if not s or not h:
        return False

    if s.startswith("*."):
        base = s[2:]
        return h.endswith("." + base)

    return normalize_host(s) == h


def tls_pair_validated(d2_host, eq_host, d2_tls, eq_tls):
    d2_sans, d2_org = d2_tls
    eq_sans, eq_org = eq_tls

    if d2_sans and eq_sans:
        if any(san_matches_host(s, eq_host) for s in d2_sans):
            return True
        if any(san_matches_host(s, d2_host) for s in eq_sans):
            return True
        if d2_sans == eq_sans:
            return True

    if d2_org and eq_org and d2_org == eq_org:
        return True

    return False


def resolve_equivalent_tls(equivalent, global_tls_index, country_tls_indexes):
    hit = lookup_tls(global_tls_index, equivalent)
    if hit:
        return hit, "D1"

    for _, idx in country_tls_indexes.items():
        hit = lookup_tls(idx, equivalent)
        if hit:
            return hit, "D2_equivalents_not_in_D1"

    return None, None


def favicon_pair_score(d2_icon, eq_icon):
    return float(favicon_similarity_embedding(d2_icon, eq_icon))


def load_favicon_cache(cache_csv):
    if not cache_csv.exists():
        return {}

    out = {}
    with cache_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = f"{row.get('country', '')}|{row.get('d2_site', '')}"
            if key != "|":
                out[key] = dict(row)
    return out


def should_reuse_cached_favicon_row(row_cache, row):
    if not row_cache:
        return False
    return row_cache.get("equivalents_raw", "") == row.equivalents_raw


def score_row_favicon(row, d2_country_index, d1_index, d2_eq_index, threshold):
    d2_icon = lookup_icon(d2_country_index, row.d2_site)
    if not d2_icon:
        return False, None, None

    best_score = -2.0
    best_pool = None
    has_d1_at_threshold = False
    has_not_in_d1_at_threshold = False
    comparisons = 0

    for eq in row.equivalents:
        eq_icon, eq_pool = resolve_equivalent_icon(eq, d1_index, d2_eq_index)
        if not eq_icon:
            continue

        score = favicon_pair_score(d2_icon, eq_icon)
        if not math.isfinite(score):
            continue

        score = max(0.0, min(1.0, float(score)))
        comparisons += 1
        if score > best_score:
            best_score = score
            best_pool = eq_pool
        if score >= threshold:
            if eq_pool == "D1":
                has_d1_at_threshold = True
            elif eq_pool == "D2_equivalents_not_in_D1":
                has_not_in_d1_at_threshold = True

    if comparisons == 0:
        return False, None, None

    final_score = best_score if best_score >= 0 else None
    chosen_pool = None
    if final_score is not None and final_score >= threshold:
        if has_d1_at_threshold:
            chosen_pool = "D1"
        elif has_not_in_d1_at_threshold:
            chosen_pool = "D2_equivalents_not_in_D1"
        else:
            chosen_pool = best_pool

    return True, final_score, chosen_pool


def pct(x, total):
    if total <= 0:
        return 0.0
    return (100.0 * x) / total


def write_summary(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "country",
        "threshold",
        "n_d2_sites",
        "n_with_favicon_comparison",
        "n_validated_favicon",
        "n_validated_favicon_from_d1",
        "n_validated_favicon_from_not_in_d1",
        "n_validated_favicon_pool_unknown",
        "n_validated_tls",
        "n_validated_any",
        "n_validated_both",
        "n_no_favicon_but_tls_validated",
        "pct_with_favicon_comparison",
        "pct_validated_favicon",
        "pct_validated_favicon_from_d1_of_favicon_validated",
        "pct_validated_favicon_from_not_in_d1_of_favicon_validated",
        "pct_validated_favicon_pool_unknown_of_favicon_validated",
        "pct_validated_favicon_from_d1",
        "pct_validated_favicon_from_not_in_d1",
        "pct_validated_tls",
        "pct_validated_any",
        "pct_validated_both",
        "pct_no_favicon_but_tls_validated",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def main():
    mapping_source = MAPPING_CSV.resolve()
    favicons_root = FAVICONS_ROOT.resolve()
    certificates_dir = CERTIFICATES_DIR.resolve()
    output_dir = OUTPUT_DIR.resolve()
    threshold = THRESHOLD

    if not mapping_source.exists():
        raise SystemExit(f"Mapping source not found: {mapping_source}")
    if not favicons_root.exists():
        raise SystemExit(f"Favicons root not found: {favicons_root}")
    if not certificates_dir.exists():
        raise SystemExit(f"Certificates dir not found: {certificates_dir}")

    rows = read_mapping_rows(mapping_source)
    if not rows:
        raise SystemExit(f"No usable rows in mapping source: {mapping_source}")

    seen = {}
    for row in rows:
        seen[country_to_code(row.country)] = None
    target_countries = sorted(seen.keys())

    rows_by_country = {code: [] for code in target_countries}
    for row in rows:
        code = country_to_code(row.country)
        if code in rows_by_country:
            rows_by_country[code].append(row)

    d1_icon_index = build_icon_index(favicons_root / "D1" / "icons")
    d2_eq_icon_index = build_icon_index(favicons_root / "D2_equivalents_not_in_D1" / "icons")

    global_tls_file = certificates_dir / "global_urls_data.json"
    if not global_tls_file.exists():
        raise SystemExit(f"Missing TLS file: {global_tls_file}")

    with global_tls_file.open("r", encoding="utf-8") as f:
        global_tls_data = json.load(f)
    global_tls_index = build_tls_index(global_tls_data)

    country_tls_indexes = {}
    for code, file_name in CERT_FILE_BY_COUNTRY_CODE.items():
        path = certificates_dir / file_name
        if not path.exists():
            country_tls_indexes[code] = {}
            continue
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        country_tls_indexes[code] = build_tls_index(data)

    cache_csv = output_dir / "d2_equivalent_best_similarity_cache.csv"
    favicon_cache = load_favicon_cache(cache_csv)

    out_rows = []
    for code in tqdm(target_countries, desc="Countries", unit="country"):
        country_rows = rows_by_country.get(code, [])
        folder = infer_d2_folder_for_country(code, favicons_root)
        d2_icon_index = build_icon_index(favicons_root / folder / "icons") if folder else {}
        d2_tls_index = country_tls_indexes.get(code, {})

        n_total = len(country_rows)
        n_with_favicon_comparison = 0
        n_validated_favicon = 0
        n_validated_favicon_from_d1 = 0
        n_validated_favicon_from_not_in_d1 = 0
        n_validated_favicon_pool_unknown = 0
        n_validated_tls = 0
        n_validated_any = 0
        n_validated_both = 0
        n_no_favicon_but_tls_validated = 0

        for row in country_rows:
            key = f"{code}|{row.d2_site}"
            cached = favicon_cache.get(key, {})

            if cached:
                has_favicon_comp = cached.get("status", "") == "compared" and int(cached.get("n_equivalents_with_favicon", "0") or 0) > 0
                score_text = (cached.get("best_score") or "").strip()
                best_pool = (cached.get("best_equivalent_pool") or "").strip()
                try:
                    best_score = float(score_text) if score_text else float("nan")
                except Exception:
                    best_score = float("nan")
            else:
                has_favicon_comp, best_score, best_pool = score_row_favicon(
                    row,
                    d2_icon_index,
                    d1_icon_index,
                    d2_eq_icon_index,
                    float(threshold),
                )

            favicon_validated = has_favicon_comp and best_score is not None and math.isfinite(best_score) and float(best_score) >= float(threshold)

            d2_tls = lookup_tls(d2_tls_index, row.d2_site)
            tls_validated = False
            if d2_tls:
                for eq in row.equivalents:
                    resolved = resolve_equivalent_tls(eq, global_tls_index, country_tls_indexes)
                    eq_tls = resolved[0]
                    if not eq_tls:
                        continue
                    if tls_pair_validated(row.d2_site, eq, d2_tls, eq_tls):
                        tls_validated = True
                        break

            if has_favicon_comp:
                n_with_favicon_comparison += 1
            if favicon_validated:
                n_validated_favicon += 1
                if best_pool == "D1":
                    n_validated_favicon_from_d1 += 1
                elif best_pool == "D2_equivalents_not_in_D1":
                    n_validated_favicon_from_not_in_d1 += 1
                else:
                    n_validated_favicon_pool_unknown += 1
            if tls_validated:
                n_validated_tls += 1
            if favicon_validated or tls_validated:
                n_validated_any += 1
            if favicon_validated and tls_validated:
                n_validated_both += 1
            if (not has_favicon_comp) and tls_validated:
                n_no_favicon_but_tls_validated += 1

        out_rows.append(
            {
                "country": code,
                "threshold": f"{float(threshold):.4f}",
                "n_d2_sites": str(n_total),
                "n_with_favicon_comparison": str(n_with_favicon_comparison),
                "n_validated_favicon": str(n_validated_favicon),
                "n_validated_favicon_from_d1": str(n_validated_favicon_from_d1),
                "n_validated_favicon_from_not_in_d1": str(n_validated_favicon_from_not_in_d1),
                "n_validated_favicon_pool_unknown": str(n_validated_favicon_pool_unknown),
                "n_validated_tls": str(n_validated_tls),
                "n_validated_any": str(n_validated_any),
                "n_validated_both": str(n_validated_both),
                "n_no_favicon_but_tls_validated": str(n_no_favicon_but_tls_validated),
                "pct_with_favicon_comparison": f"{pct(n_with_favicon_comparison, n_total):.2f}",
                "pct_validated_favicon": f"{pct(n_validated_favicon, n_total):.2f}",
                "pct_validated_favicon_from_d1_of_favicon_validated": f"{pct(n_validated_favicon_from_d1, n_validated_favicon):.2f}",
                "pct_validated_favicon_from_not_in_d1_of_favicon_validated": f"{pct(n_validated_favicon_from_not_in_d1, n_validated_favicon):.2f}",
                "pct_validated_favicon_pool_unknown_of_favicon_validated": f"{pct(n_validated_favicon_pool_unknown, n_validated_favicon):.2f}",
                "pct_validated_favicon_from_d1": f"{pct(n_validated_favicon_from_d1, n_total):.2f}",
                "pct_validated_favicon_from_not_in_d1": f"{pct(n_validated_favicon_from_not_in_d1, n_total):.2f}",
                "pct_validated_tls": f"{pct(n_validated_tls, n_total):.2f}",
                "pct_validated_any": f"{pct(n_validated_any, n_total):.2f}",
                "pct_validated_both": f"{pct(n_validated_both, n_total):.2f}",
                "pct_no_favicon_but_tls_validated": f"{pct(n_no_favicon_but_tls_validated, n_total):.2f}",
            }
        )

    out_name = f"d2_favicon_tls_joint_summary_t{float(threshold):.4f}.csv"
    out_csv = output_dir / out_name
    write_summary(out_csv, out_rows)
    print(f"Summary saved: {out_csv}")


if __name__ == "__main__":
    main()
