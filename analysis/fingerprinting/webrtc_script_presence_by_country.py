

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

CURRENT_DIR = Path(__file__).resolve().parent
GROUP_ROOT = CURRENT_DIR / "reports_D1" / "groups"
DEFAULT_GROUP_LABEL = "ALGERIA|INDIA|UAE__vs__GERMANY|USA"
TARGET_COUNTRIES = ["USA", "GERMANY"]


def parse_group_label(label: str):
    if "__vs__" not in label:
        raise ValueError(
            "Group label must follow the pattern 'COUNTRY1|COUNTRY2__vs__COUNTRY3|COUNTRY4'"
        )
    left, right = label.split("__vs__", 1)
    group_a = tuple(country.strip() for country in left.split("|") if country.strip())
    group_b = tuple(country.strip() for country in right.split("|") if country.strip())
    if not group_a or not group_b:
        raise ValueError(f"Invalid group label: {label}")
    return group_a, group_b


def load_engine():
    load_dotenv()
    return create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )


def load_sites_from_group_report(group_label):
    group_dir = GROUP_ROOT / group_label
    report_path = group_dir / "webrtc_any_country_focus_report.txt"

    sites = []
    for line in report_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("- "):
            sites.append(line[2:].strip())
    return sorted(set(sites))


def query_country_script_urls(engine, etld: str, country: str):
    q = text(
        """
        SELECT DISTINCT cs.etld, cs.location, s.script_src
        FROM crawl_sessions cs
        LEFT JOIN scripts s ON s.session_id = cs.id
        WHERE cs.etld = :etld
          AND cs.location = :country
          AND s.script_src IS NOT NULL
          AND TRIM(s.script_src) <> ''
        """
    )
    df = pd.read_sql(q, engine, params={"etld": etld, "country": country})
    if df.empty:
        return set()
    return set(df["script_src"].dropna().astype(str).tolist())


def query_group_a_script_urls(engine, etld: str, countries):
    q = text(
        """
        SELECT DISTINCT cs.etld, cs.location, s.script_src
        FROM crawl_sessions cs
        LEFT JOIN scripts s ON s.session_id = cs.id
        WHERE cs.etld = :etld
          AND cs.location IN :countries
          AND s.script_src IS NOT NULL
          AND TRIM(s.script_src) <> ''
        """
    )
    df = pd.read_sql(q, engine, params={"etld": etld, "countries": tuple(countries)})
    if df.empty:
        return set()
    return set(df["script_src"].dropna().astype(str).tolist())


def analyze_country(engine, sites, group_a, target_country):
    rows = []
    for etld in sites:
        a_urls = query_group_a_script_urls(engine, etld, group_a)
        target_urls = query_country_script_urls(engine, etld, target_country)
        overlap = a_urls & target_urls
        rows.append(
            {
                "etld": etld,
                "group_a_countries": "|".join(group_a),
                "target_country": target_country,
                "n_a_scripts": len(a_urls),
                "n_target_scripts": len(target_urls),
                "n_overlap_scripts": len(overlap),
                "loaded_in_target_country": bool(overlap),
                "shared_script_srcs": sorted(overlap),
            }
        )
    return pd.DataFrame(rows)


def summarize(frame: pd.DataFrame, target_country: str):
    loaded = int(frame["loaded_in_target_country"].sum())
    not_loaded = int((~frame["loaded_in_target_country"]).sum())
    total = len(frame)
    return {
        "target_country": target_country,
        "n_sites": total,
        "loaded": loaded,
        "not_loaded": not_loaded,
        "pct_loaded": round((loaded / total * 100) if total else 0.0, 2),
        "pct_not_loaded": round((not_loaded / total * 100) if total else 0.0, 2),
    }


def main():
    group_a, group_b = parse_group_label(DEFAULT_GROUP_LABEL)
    engine = load_engine()
    sites = load_sites_from_group_report(DEFAULT_GROUP_LABEL)
    out_dir = GROUP_ROOT / DEFAULT_GROUP_LABEL
    out_dir.mkdir(parents=True, exist_ok=True)

    for country in TARGET_COUNTRIES:
        result = analyze_country(engine, sites, group_a, country)
        summary = summarize(result, country)
        out_csv = out_dir / f"webrtc_script_presence_{country}.csv"
        result.to_csv(out_csv, index=False)

        print(f"{country}: loaded={summary['loaded']}, not_loaded={summary['not_loaded']}, total={summary['n_sites']}")
        print(f"  loaded_pct={summary['pct_loaded']:.2f}%")
        print(f"  not_loaded_pct={summary['pct_not_loaded']:.2f}%")
        print(f"  csv={out_csv}")

        not_loaded_sites = result[result["loaded_in_target_country"] == False]["etld"].tolist()
        if not_loaded_sites:
            print(f"  sample_not_loaded={not_loaded_sites[:10]}")


if __name__ == "__main__":
    main()
