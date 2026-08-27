"""Compute D2 favicon similarity stats against equivalent sites."""

import ast
import csv
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

from compare_favicon_embedding import favicon_similarity_embedding


_THIS_DIR = Path(__file__).resolve().parent
_VALIDATE_D2_DIR = _THIS_DIR.parent
_REPO_ROOT = _VALIDATE_D2_DIR.parent.parent

DEFAULT_MAPPING_CSV = _REPO_ROOT / "site_catalogs_construction" / "catalogs" / "D2_to_global"
DEFAULT_FAVICONS_ROOT = _REPO_ROOT / "data" / "favicons"
DEFAULT_OUTPUT_DIR = _VALIDATE_D2_DIR / "results"
DEFAULT_THRESHOLD = 0.85
DEFAULT_WORKERS = 1
SHOW_PROGRESS = False

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
    "germany": "de",
    "usa": "us",
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
    host = host.split("/", 1)[0]
    host = host.strip(".")
    return host


def host_candidates(host):
    h = normalize_host(host)
    if not h:
        return []
    cands = [h]
    if h.startswith("www."):
        cands.append(h[4:])
    else:
        cands.append("www." + h)
    return list(dict.fromkeys(cands))


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


def country_to_code(country_value):
    c = normalize_host(country_value)
    if len(c) == 2:
        return c
    return COUNTRY_ALIASES_TO_CODE.get(c, c)


def _country_from_d2_to_global_name(path):
    stem = path.stem.lower()
    prefix = "d2_to_global_"
    if stem.startswith(prefix):
        return country_to_code(stem[len(prefix):])
    return ""


def _rows_from_country_pair_csv(csv_path, country_code):
    grouped = defaultdict(list)
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


def cache_key(country, d2_site):
    return f"{country}|{d2_site}"


def load_cache(cache_csv):
    if not cache_csv.exists():
        return {}
    with cache_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        out = {}
        for row in reader:
            key = cache_key(row.get("country", ""), row.get("d2_site", ""))
            if key != "|":
                out[key] = dict(row)
        return out


def write_cache(cache_csv, rows):
    cache_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "country",
        "d2_site",
        "equivalents_raw",
        "status",
        "n_equivalents_total",
        "n_equivalents_with_favicon",
        "best_equivalent",
        "best_equivalent_icon_path",
        "best_score",
        "d2_icon_path",
    ]
    with cache_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def resolve_equivalent_icon(equivalent, d1_index, d2_eq_index):
    hit = lookup_icon(d1_index, equivalent)
    if hit:
        return hit, "D1"
    hit = lookup_icon(d2_eq_index, equivalent)
    if hit:
        return hit, "D2_equivalents_not_in_D1"
    return None, None


def evaluate_row(row, d2_country_index, d1_index, d2_eq_index):
    d2_icon = lookup_icon(d2_country_index, row.d2_site)
    if not d2_icon:
        return {
            "country": row.country,
            "d2_site": row.d2_site,
            "equivalents_raw": row.equivalents_raw,
            "status": "no_d2_favicon",
            "n_equivalents_total": str(len(row.equivalents)),
            "n_equivalents_with_favicon": "0",
            "best_equivalent": "",
            "best_equivalent_icon_path": "",
            "best_score": "",
            "d2_icon_path": "",
        }

    available = []
    for eq in row.equivalents:
        eq_icon, _ = resolve_equivalent_icon(eq, d1_index, d2_eq_index)
        if eq_icon:
            available.append((eq, eq_icon))

    if not available:
        return {
            "country": row.country,
            "d2_site": row.d2_site,
            "equivalents_raw": row.equivalents_raw,
            "status": "no_equivalent_favicon",
            "n_equivalents_total": str(len(row.equivalents)),
            "n_equivalents_with_favicon": "0",
            "best_equivalent": "",
            "best_equivalent_icon_path": "",
            "best_score": "",
            "d2_icon_path": d2_icon,
        }

    best_equivalent = ""
    best_equivalent_icon_path = ""
    best_score = -2.0
    for eq_site, eq_icon in available:
        score = favicon_similarity_embedding(d2_icon, eq_icon)
        if math.isnan(score):
            continue
        if score > best_score:
            best_score = score
            best_equivalent = eq_site
            best_equivalent_icon_path = eq_icon

    return {
        "country": row.country,
        "d2_site": row.d2_site,
        "equivalents_raw": row.equivalents_raw,
        "status": "compared",
        "n_equivalents_total": str(len(row.equivalents)),
        "n_equivalents_with_favicon": str(len(available)),
        "best_equivalent": best_equivalent,
        "best_equivalent_icon_path": best_equivalent_icon_path,
        "best_score": f"{best_score:.8f}",
        "d2_icon_path": d2_icon,
    }


def summarize_country(rows, threshold):
    rows_list = list(rows)
    n_total = len(rows_list)
    if n_total == 0:
        return {
            "n_sites": "0",
            "n_no_d2_favicon": "0",
            "n_no_equivalent_favicon": "0",
            "n_similar": "0",
            "n_not_similar": "0",
            "frac_no_d2_favicon": "0.000000",
            "frac_no_equivalent_favicon": "0.000000",
            "frac_similar": "0.000000",
            "frac_not_similar": "0.000000",
            "pct_no_d2_favicon": "0.00",
            "pct_no_equivalent_favicon": "0.00",
            "pct_similar": "0.00",
            "pct_not_similar": "0.00",
            "sum_check_pct": "0.00",
        }

    n_a = 0
    n_b = 0
    n_c = 0
    n_d = 0
    for row in rows_list:
        status = row.get("status", "")
        if status == "no_d2_favicon":
            n_a += 1
            continue
        if status == "no_equivalent_favicon":
            n_b += 1
            continue

        score_text = (row.get("best_score") or "").strip()
        score = float(score_text) if score_text else float("-inf")
        if score >= threshold:
            n_c += 1
        else:
            n_d += 1

    if n_a + n_b + n_c + n_d != n_total:
        raise RuntimeError("Outcome partition mismatch: counts do not sum to total")

    def frac(x):
        return x / n_total

    def pct(x):
        return 100.0 * x / n_total

    sum_pct = pct(n_a) + pct(n_b) + pct(n_c) + pct(n_d)

    return {
        "n_sites": str(n_total),
        "n_no_d2_favicon": str(n_a),
        "n_no_equivalent_favicon": str(n_b),
        "n_similar": str(n_c),
        "n_not_similar": str(n_d),
        "frac_no_d2_favicon": f"{frac(n_a):.6f}",
        "frac_no_equivalent_favicon": f"{frac(n_b):.6f}",
        "frac_similar": f"{frac(n_c):.6f}",
        "frac_not_similar": f"{frac(n_d):.6f}",
        "pct_no_d2_favicon": f"{pct(n_a):.2f}",
        "pct_no_equivalent_favicon": f"{pct(n_b):.2f}",
        "pct_similar": f"{pct(n_c):.2f}",
        "pct_not_similar": f"{pct(n_d):.2f}",
        "sum_check_pct": f"{sum_pct:.2f}",
    }


def write_summary(summary_csv, rows):
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "country",
        "threshold",
        "n_sites",
        "n_no_d2_favicon",
        "n_no_equivalent_favicon",
        "n_similar",
        "n_not_similar",
        "frac_no_d2_favicon",
        "frac_no_equivalent_favicon",
        "frac_similar",
        "frac_not_similar",
        "pct_no_d2_favicon",
        "pct_no_equivalent_favicon",
        "pct_similar",
        "pct_not_similar",
        "sum_check_pct",
    ]
    with summary_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def parse_countries_arg(text):
    if not text.strip():
        return []
    out = []
    for part in text.split(","):
        code = country_to_code(part)
        if code:
            out.append(code)
    return list(dict.fromkeys(out))


def main():
    mapping_source = DEFAULT_MAPPING_CSV.resolve()
    favicons_root = DEFAULT_FAVICONS_ROOT.resolve()
    output_dir = DEFAULT_OUTPUT_DIR.resolve()

    rows = read_mapping_rows(mapping_source)

    seen = {}
    for row in rows:
        seen[country_to_code(row.country)] = None
    target_countries = list(seen.keys())

    rows_by_country = {c: [] for c in target_countries}
    for row in rows:
        code = country_to_code(row.country)
        if code in rows_by_country:
            rows_by_country[code].append(row)

    d1_index = build_icon_index(favicons_root / "D1" / "icons")
    d2_eq_index = build_icon_index(favicons_root / "D2_equivalents_not_in_D1" / "icons")

    cache_csv = output_dir / "d2_favicon_similarity_cache.csv"
    summary_csv = output_dir / f"d2_favicon_similarity_summary_t{DEFAULT_THRESHOLD:.4f}.csv"
    existing_cache = load_cache(cache_csv)
    all_cache_rows = dict(existing_cache)


    for code in tqdm(target_countries, desc="Countries", unit="country") :
        country_rows = rows_by_country.get(code, [])
        folder = infer_d2_folder_for_country(code, favicons_root)
        if not folder:
            print(f"[warn] skip country={code}: cannot map to D2 folder under {favicons_root}")
            continue

        d2_icon_dir = favicons_root / folder / "icons"
        if not d2_icon_dir.exists():
            print(f"[warn] country={code}: icon dir not found: {d2_icon_dir}")

        d2_index = build_icon_index(d2_icon_dir)

        to_compute = []
        for row in country_rows:
            key = cache_key(code, row.d2_site)
            if key not in existing_cache:
                to_compute.append(row)

        if to_compute:
            workers = max(1, int(DEFAULT_WORKERS))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {
                    ex.submit(evaluate_row, row, d2_index, d1_index, d2_eq_index): row for row in to_compute
                }
                row_progress = tqdm(total=len(futures), desc=f"{code} rows", unit="row", leave=False)
                for future in as_completed(futures):
                    row = futures[future]
                    all_cache_rows[cache_key(code, row.d2_site)] = future.result()
                    if row_progress is not None:
                        row_progress.update(1)
                if row_progress is not None:
                    row_progress.close()

        cache_rows_sorted = sorted(all_cache_rows.values(), key=lambda r: (r.get("country", ""), r.get("d2_site", "")))
        write_cache(cache_csv, cache_rows_sorted)

    cache_rows_final = sorted(all_cache_rows.values(), key=lambda r: (r.get("country", ""), r.get("d2_site", "")))
    summary_rows = []
    for code in target_countries:
        country_cache_rows = [r for r in cache_rows_final if country_to_code(r.get("country", "")) == code]
        if not country_cache_rows:
            continue
        stats = summarize_country(country_cache_rows, DEFAULT_THRESHOLD)
        stats["country"] = code
        stats["threshold"] = f"{DEFAULT_THRESHOLD:.4f}"
        summary_rows.append(stats)

    write_summary(summary_csv, summary_rows)
    print(f"Cache written: {cache_csv}")
    print(f"Summary written: {summary_csv}")


if __name__ == "__main__":
    main()
