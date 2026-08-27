import json
import os
import sys
from collections import defaultdict

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
TOP_FINGERPRINTERS_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "top_fingerprinters")
COUNTRY_SPECIFIC_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "country_specific")
D3_COUNTRY_DATASETS_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "d3_country_datasets")
COUNTRIES = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
TOP_K = 10

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TOP_FINGERPRINTERS_OUTPUT_DIR, exist_ok=True)
os.makedirs(COUNTRY_SPECIFIC_OUTPUT_DIR, exist_ok=True)
os.makedirs(D3_COUNTRY_DATASETS_OUTPUT_DIR, exist_ok=True)


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
    if isinstance(entry, str):
        value = entry.strip()
        if not value:
            return None
        domain = extract_domain_from_url(value)
        return domain or value

    if not isinstance(entry, dict):
        return None

    for key in ("caller_url", "script_src", "script_url", "domain"):
        value = entry.get(key)
        if not value:
            continue
        value = str(value).strip()
        if value.startswith("//"):
            value = "https:" + value
        domain = extract_domain_from_url(value)
        if domain:
            return domain
        if value:
            return value

    return None


def build_site_fingerprinting_table(sessions):
    rows = []
    for _, row in sessions.iterrows():
        fp_data = row.get("fingerprinting") or {}
        if not isinstance(fp_data, dict):
            continue

        for fp_type, entries in fp_data.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                fingerprinter = _candidate_fingerprinter(entry)
                if not fingerprinter:
                    continue
                rows.append(
                    {
                        "etld": row["etld"],
                        "category": row["category"],
                        "location": row["location"],
                        "fp_type": fp_type,
                        "fingerprinter": fingerprinter,
                    }
                )

    if not rows:
        return pd.DataFrame(columns=["etld", "category", "location", "fp_type", "fingerprinter"])

    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["etld", "category", "location", "fp_type", "fingerprinter"])[
        ["etld", "category", "location", "fp_type", "fingerprinter"]
    ]


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
    for category in ["global", "country_coded", "country_specific", "D2_EQUIVALENT"]:
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
    categories = ["global", "country_coded", "country_specific", "D2_EQUIVALENT", "overall (D1/D2/D3)"]
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
# Top fingerprinters by country
# ---------------------------------------------------------------------------
def compute_top_fingerprinters_for_subset(sessions, site_fingerprinting):
    denominators = sessions[["location", "etld"]].drop_duplicates().groupby("location").size().to_dict()
    presence = defaultdict(set)
    for _, row in site_fingerprinting.iterrows():
        presence[(row["location"], row["fingerprinter"])].add(row["etld"])

    rows = []
    for (loc, fingerprinter), etlds in presence.items():
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue
        rows.append(
            {
                "location": loc,
                "fingerprinter": fingerprinter,
                "percentage": round(len(etlds) / denom * 100, 2),
            }
        )

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return pd.DataFrame(columns=["Rank"] + COUNTRIES), pd.DataFrame(columns=["Fingerprinter"] + COUNTRIES)

    ranked = ranked.sort_values(["location", "percentage", "fingerprinter"], ascending=[True, False, True])
    ranked["rank"] = ranked.groupby("location").cumcount() + 1
    top_ranked = ranked[ranked["rank"] <= TOP_K].copy()
    top_ranked["entry"] = top_ranked.apply(lambda r: f"{r['fingerprinter']} ({r['percentage']:.2f}%)", axis=1)

    list_df = (
        top_ranked.pivot(index="rank", columns="location", values="entry")
        .reindex(index=range(1, TOP_K + 1), columns=COUNTRIES)
        .reset_index()
        .rename(columns={"rank": "Rank"})
    )

    fp_rank = pd.concat(
        [
            ranked.groupby("fingerprinter")["location"].nunique().rename("n_countries"),
            ranked.groupby("fingerprinter")["percentage"].mean().rename("avg_pct"),
        ],
        axis=1,
    ).sort_values(["n_countries", "avg_pct"], ascending=[False, False])
    top_fps = fp_rank.head(TOP_K).index.tolist()

    matrix_df = (
        ranked[ranked["fingerprinter"].isin(top_fps)]
        .pivot_table(index="fingerprinter", columns="location", values="percentage", aggfunc="first")
        .reindex(index=top_fps, columns=COUNTRIES)
        .reset_index()
        .rename(columns={"fingerprinter": "Fingerprinter"})
    )

    return list_df, matrix_df


def write_top_fingerprinter_outputs(sessions, site_fingerprinting):
    bucket_map = {
        "overall_d1_d2_d3": (
            sessions[sessions["category"].isin(["global", "country_coded", "country_specific"])].copy(),
            site_fingerprinting[site_fingerprinting["category"].isin(["global", "country_coded", "country_specific"])].copy(),
        ),
        "d1": (
            sessions[sessions["category"] == "global"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "global"].copy(),
        ),
        "d2": (
            sessions[sessions["category"] == "country_coded"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "country_coded"].copy(),
        ),
        "d3": (
            sessions[sessions["category"] == "country_specific"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "country_specific"].copy(),
        ),
        "d2_equivalent": (
            sessions[sessions["category"] == "D2_EQUIVALENT"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "D2_EQUIVALENT"].copy(),
        ),
    }

    for bucket_name, (bucket_sessions, bucket_fps) in bucket_map.items():
        list_df, matrix_df = compute_top_fingerprinters_for_subset(bucket_sessions, bucket_fps)
        list_df.to_csv(f"{TOP_FINGERPRINTERS_OUTPUT_DIR}/top_fingerprinters_{bucket_name}.csv", index=False)
        matrix_df.to_csv(f"{TOP_FINGERPRINTERS_OUTPUT_DIR}/top_fingerprinters_matrix_{bucket_name}.csv", index=False)


# ---------------------------------------------------------------------------
# Country-specific fingerprinters
# ---------------------------------------------------------------------------
def compute_country_specific_fingerprinters(site_fingerprinting):
    presence = defaultdict(set)
    for _, row in site_fingerprinting.iterrows():
        presence[(row["location"], row["fingerprinter"])].add(row["etld"])

    tracker_locations = defaultdict(set)
    for (loc, fingerprinter) in presence:
        tracker_locations[fingerprinter].add(loc)

    total_occurrences = (
        site_fingerprinting.groupby(["location", "fingerprinter", "etld"])
        .size()
        .reset_index()
        .groupby("location")
        .size()
        .to_dict()
    )

    summary = []
    for loc in COUNTRIES:
        local_only = {
            fingerprinter
            for (presence_loc, fingerprinter) in presence
            if presence_loc == loc and len(tracker_locations[fingerprinter]) == 1
        }
        occ = sum(len(presence[(loc, fingerprinter)]) for fingerprinter in local_only)
        total = total_occurrences.get(loc, 0)
        summary.append(
            {
                "country": loc,
                "num_country_specific_fingerprinters": len(local_only),
                "coverage_pct": round(occ / total * 100 if total > 0 else 0, 2),
            }
        )
    return pd.DataFrame(summary)


def compute_country_specific_top_fingerprinters(sessions, site_fingerprinting, countries, top_k):
    denominators = sessions[["location", "etld"]].drop_duplicates().groupby("location").size().to_dict()
    presence = defaultdict(set)
    for _, row in site_fingerprinting.iterrows():
        presence[(row["location"], row["fingerprinter"])].add(row["etld"])

    tracker_locations = defaultdict(set)
    for (loc, fingerprinter) in presence:
        tracker_locations[fingerprinter].add(loc)

    rows = []
    for (loc, fingerprinter), etlds in presence.items():
        if len(tracker_locations[fingerprinter]) != 1:
            continue
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue
        rows.append(
            {
                "location": loc,
                "fingerprinter": fingerprinter,
                "percentage": round(len(etlds) / denom * 100, 2),
            }
        )

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return pd.DataFrame(columns=countries)

    ranked = ranked.sort_values(["location", "percentage", "fingerprinter"], ascending=[True, False, True])
    result = {}
    for loc in countries:
        country_rows = ranked[ranked["location"] == loc].head(top_k)
        result[loc] = [f"{fp} ({pct:.2f}%)" for fp, pct in zip(country_rows["fingerprinter"], country_rows["percentage"])]

    return pd.DataFrame({country: pd.Series(vals) for country, vals in result.items()}).reindex(columns=countries)


def write_country_specific_outputs(sessions, site_fingerprinting):
    compute_country_specific_fingerprinters(site_fingerprinting).to_csv(
        f"{COUNTRY_SPECIFIC_OUTPUT_DIR}/country_specific_fingerprinters_summary.csv",
        index=False,
    )

    bucket_map = {
        "overall_d1_d2_d3": (
            sessions[sessions["category"].isin(["global", "country_coded", "country_specific"])].copy(),
            site_fingerprinting[site_fingerprinting["category"].isin(["global", "country_coded", "country_specific"])].copy(),
        ),
        "d1": (
            sessions[sessions["category"] == "global"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "global"].copy(),
        ),
        "d2": (
            sessions[sessions["category"] == "country_coded"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "country_coded"].copy(),
        ),
        "d3": (
            sessions[sessions["category"] == "country_specific"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "country_specific"].copy(),
        ),
        "d2_equivalent": (
            sessions[sessions["category"] == "D2_EQUIVALENT"].copy(),
            site_fingerprinting[site_fingerprinting["category"] == "D2_EQUIVALENT"].copy(),
        ),
    }

    for bucket_name, (bucket_sessions, bucket_fps) in bucket_map.items():
        list_df = compute_country_specific_top_fingerprinters(bucket_sessions, bucket_fps, COUNTRIES, TOP_K)
        list_df.to_csv(f"{COUNTRY_SPECIFIC_OUTPUT_DIR}/top_country_specific_fingerprinters_{bucket_name}.csv", index=False)


def _fingerprinting_flag(value):
    if pd.isna(value):
        return 0
    if isinstance(value, str):
        return int(value.strip().lower() in {"1", "true", "t", "yes", "y"})
    return int(bool(value))


def write_d3_country_fingerprinting_summary(sessions):
    """Write one D3 CSV per country with one row per (location, site) and OR-aggregated is_fingerprinting."""
    d3_sessions = sessions[sessions["category"] == "country_specific"].copy()
    if d3_sessions.empty:
        for country in COUNTRIES:
            out_path = os.path.join(D3_COUNTRY_DATASETS_OUTPUT_DIR, f"d3_fingerprinting_{country}.csv")
            pd.DataFrame(columns=["location", "site", "is_fingerprinting"]).to_csv(out_path, index=False)
        return

    keys = (
        d3_sessions[["location", "etld"]]
        .drop_duplicates()
        .rename(columns={"etld": "site"})
    )

    agg = (
        d3_sessions.groupby(["location", "etld"], as_index=False)["is_fingerprinting"]
        .agg(lambda values: int(any(_fingerprinting_flag(v) for v in values)))
        .rename(columns={"etld": "site"})
    )

    site_flags = keys.merge(agg, on=["location", "site"], how="left")
    site_flags["is_fingerprinting"] = site_flags["is_fingerprinting"].fillna(0).astype(int)

    for country in COUNTRIES:
        country_df = (
            site_flags[site_flags["location"] == country]
            .sort_values(["site"])
            .reset_index(drop=True)
        )
        out_path = os.path.join(D3_COUNTRY_DATASETS_OUTPUT_DIR, f"d3_fingerprinting_{country}.csv")
        country_df.to_csv(out_path, index=False)


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
    write_top_fingerprinter_outputs(sessions, site_fingerprinting)
    write_country_specific_outputs(sessions, site_fingerprinting)
    write_d3_country_fingerprinting_summary(sessions)


if __name__ == "__main__":
    main()

