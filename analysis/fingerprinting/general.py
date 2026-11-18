import json
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from analysis.utils.sessions import filter_global_to_common_etlds
from analysis.utils.urls import extract_domain_from_url

OUTPUT_DIR = os.path.join(CURRENT_DIR, "reports_general")
COUNTRIES = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_tables():
    load_dotenv()
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}/"
        f"{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )

    sessions = pd.read_sql(
        """
        SELECT
            id AS session_id,
            etld,
            url,
            category,
            location,
            access_id,
            fingerprinting,
            is_fingerprinting
        FROM crawl_sessions
        WHERE location NOT LIKE '%%VPN'
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
        """,
        engine,
    )

    sessions["fingerprinting"] = sessions["fingerprinting"].apply(
        lambda value: json.loads(value) if isinstance(value, str) and value.strip() else {}
    )
    return sessions


# ---------------------------------------------------------------------------
# Site-level fingerprinting universe
# ---------------------------------------------------------------------------
def _candidate_fingerprinter(entry):
    if not isinstance(entry, dict):
        return None

    url = entry.get("caller_url")
    if not url:
        return None

    return extract_domain_from_url(url) or url


def build_site_fingerprinting_table(sessions):
    """
    Intersection-only approach:
    For each site (etld, category, location):
      1. Group by access_id
      2. For each access_id, determine if it has fingerprinting
      3. Keep only fingerprinters that appear in ALL access_ids
    
    Output: One row per (etld, category, location, fp_type, fingerprinter)
            where fingerprinter appears consistently across ALL access_ids for that site
    """
    rows = []
    
    for (etld, category, location), site_sessions in sessions.groupby(["etld", "category", "location"]):
        access_ids = site_sessions["access_id"].unique()
        
        # For each access_id, collect fingerprinters
        fingerprinters_by_access_id = {}
        
        for access_id in access_ids:
            access_sessions = site_sessions[site_sessions["access_id"] == access_id]
            access_fingerprinters = {}
            
            for _, row in access_sessions.iterrows():
                fp_data = row.get("fingerprinting") or {}
                if not isinstance(fp_data, dict):
                    continue
                
                for fp_type, entries in fp_data.items():
                    if not isinstance(entries, list):
                        continue
                    if fp_type not in access_fingerprinters:
                        access_fingerprinters[fp_type] = set()
                    
                    for entry in entries:
                        fingerprinter = _candidate_fingerprinter(entry)
                        if fingerprinter:
                            access_fingerprinters[fp_type].add(fingerprinter)
            
            fingerprinters_by_access_id[access_id] = access_fingerprinters
        
        # Find intersection: fingerprinters present in ALL access_ids
        if not fingerprinters_by_access_id:
            continue
        
        all_fp_types = set()
        for fingerprinters in fingerprinters_by_access_id.values():
            all_fp_types.update(fingerprinters.keys())
        
        for fp_type in all_fp_types:
            # Get fingerprinters for this type in each access_id
            fp_sets = [
                fingerprinters_by_access_id.get(aid, {}).get(fp_type, set())
                for aid in access_ids
            ]
            
            # Intersection: only fingerprinters present in all access_ids
            if all(fp_sets):  # All access_ids have this fp_type
                common_fingerprinters = set.intersection(*fp_sets)
                for fingerprinter in common_fingerprinters:
                    rows.append({
                        "etld": etld,
                        "category": category,
                        "location": location,
                        "fp_type": fp_type,
                        "fingerprinter": fingerprinter,
                    })
    
    if not rows:
        empty = pd.DataFrame(columns=["etld", "category", "location", "fp_type", "fingerprinter"])
        empty.to_csv(f"{OUTPUT_DIR}/site_level_summary.csv", index=False)
        return empty
    
    df = pd.DataFrame(rows)
    df = df[["etld", "category", "location", "fp_type", "fingerprinter"]]
    df = df.sort_values(["location", "category", "etld", "fp_type", "fingerprinter"], kind="mergesort").reset_index(drop=True)
    df.to_csv(f"{OUTPUT_DIR}/site_level_summary.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------
def compute_fingerprinting_prevalence(sessions, site_fingerprinting):
    all_etlds = sessions[["etld", "category", "location"]].drop_duplicates()
    fp_etlds = (
        site_fingerprinting[["etld", "category", "location"]]
        .drop_duplicates()
        .assign(has_fingerprinting=1)
    )

    merged = all_etlds.merge(fp_etlds, on=["etld", "category", "location"], how="left").fillna({"has_fingerprinting": 0})
    prevalence = (
        merged.groupby(["category", "location"])["has_fingerprinting"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
    )

    overall = merged[merged["category"].isin(["global", "country_coded", "country_specific"])].copy()
    overall_prevalence = (
        overall.groupby("location")["has_fingerprinting"]
        .mean()
        .multiply(100)
        .round(2)
    )
    overall_row = pd.DataFrame([overall_prevalence], index=["overall (D1/D2/D3)"])
    return pd.concat([prevalence, overall_row], axis=0)


def compute_fingerprinting_type_prevalence(sessions, site_fingerprinting):
    all_sites = sessions[["etld", "category", "location"]].drop_duplicates()
    fp_types = site_fingerprinting[["etld", "category", "location", "fp_type"]].drop_duplicates()
    fp_type_values = pd.Index(fp_types["fp_type"].dropna().unique())

    if fp_type_values.empty:
        return pd.DataFrame(columns=["category", "fp_type", *COUNTRIES])

    site_type_grid = all_sites.assign(_key=1).merge(
        pd.DataFrame({"fp_type": fp_type_values, "_key": 1}),
        on="_key",
        how="inner",
    ).drop(columns=["_key"])

    merged = site_type_grid.merge(
        fp_types.assign(has_fingerprint_type=1),
        on=["etld", "category", "location", "fp_type"],
        how="left",
    ).fillna({"has_fingerprint_type": 0})

    rows = []
    for category in ["global", "country_coded", "country_specific"]:
        category_df = merged[merged["category"] == category].copy()
        if category_df.empty:
            continue
        by_type = (
            category_df.groupby(["fp_type", "location"])["has_fingerprint_type"]
            .mean()
            .unstack()
            .multiply(100)
            .round(2)
        )
        for fp_type, row in by_type.iterrows():
            out = {"category": category, "fp_type": fp_type}
            for country in COUNTRIES:
                out[country] = row.get(country, 0.0)
            rows.append(out)

    overall = merged[merged["category"].isin(["global", "country_coded", "country_specific"])].copy()
    by_type_overall = (
        overall.groupby(["fp_type", "location"])["has_fingerprint_type"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
    )
    for fp_type, row in by_type_overall.iterrows():
        out = {"category": "overall (D1/D2/D3)", "fp_type": fp_type}
        for country in COUNTRIES:
            out[country] = row.get(country, 0.0)
        rows.append(out)

    return pd.DataFrame(rows)


def compute_avg_fingerprinters_per_site(site_fingerprinting):
    per_site = (
        site_fingerprinting.groupby(["category", "location", "etld"])["fingerprinter"]
        .nunique()
        .reset_index(name="num_fingerprinters")
    )

    avg = (
        per_site.groupby(["category", "location"])["num_fingerprinters"]
        .mean()
        .unstack()
        .round(2)
    )

    d1_d2_d3 = site_fingerprinting[site_fingerprinting["category"].isin(["global", "country_coded", "country_specific"])].copy()
    overall_avg = (
        d1_d2_d3.groupby(["location", "etld"])["fingerprinter"]
        .nunique()
        .groupby("location")
        .mean()
        .round(2)
    )
    overall_row = pd.DataFrame([overall_avg], index=["overall (D1/D2/D3)"])
    return pd.concat([avg, overall_row], axis=0)


def compute_total_distinct_fingerprinters(site_fingerprinting):
    total = (
        site_fingerprinting.groupby(["category", "location"])["fingerprinter"]
        .nunique()
        .unstack()
        .fillna(0)
        .astype(int)
    )

    d1_d2_d3 = site_fingerprinting[site_fingerprinting["category"].isin(["global", "country_coded", "country_specific"])].copy()
    overall_total = d1_d2_d3.groupby("location")["fingerprinter"].nunique()
    overall_row = pd.DataFrame([overall_total], index=["overall (D1/D2/D3)"])
    return pd.concat([total, overall_row], axis=0).fillna(0).astype(int)


def write_fingerprinting_summary(prevalence, avg_fps, total_fps, type_prevalence=None):
    categories = ["global", "country_coded", "country_specific", "overall (D1/D2/D3)"]
    panel_map = {
        "fingerprinting_prevalence_pct": prevalence,
        "avg_num_fingerprinters_per_site": avg_fps,
        "total_num_distinct_fingerprinters": total_fps,
    }

    rows = []
    for metric, metric_df in panel_map.items():
        for category in categories:
            row = {"metric": metric, "category": category}
            for country in COUNTRIES:
                value = None
                if category in metric_df.index and country in metric_df.columns:
                    value = metric_df.loc[category, country]
                row[country] = value
            rows.append(row)

    if type_prevalence is not None and not type_prevalence.empty:
        for _, row in type_prevalence.iterrows():
            out = {
                "metric": "fingerprinting_type_prevalence_pct",
                "category": row["category"],
                "fp_type": row["fp_type"],
            }
            for country in COUNTRIES:
                out[country] = row.get(country, None)
            rows.append(out)

    pd.DataFrame(rows).to_csv(f"{OUTPUT_DIR}/fingerprinting_summary.csv", index=False)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    sessions = load_tables()
    site_fingerprinting = build_site_fingerprinting_table(sessions)
    sessions, site_fingerprinting = filter_global_to_common_etlds(sessions, site_fingerprinting)

    prevalence = compute_fingerprinting_prevalence(sessions, site_fingerprinting)
    type_prevalence = compute_fingerprinting_type_prevalence(sessions, site_fingerprinting)
    avg_fps = compute_avg_fingerprinters_per_site(site_fingerprinting)
    total_fps = compute_total_distinct_fingerprinters(site_fingerprinting)
    write_fingerprinting_summary(prevalence, avg_fps, total_fps, type_prevalence)

    type_prevalence.to_csv(f"{OUTPUT_DIR}/fingerprinting_type_prevalence.csv", index=False)


if __name__ == "__main__":
    main()

