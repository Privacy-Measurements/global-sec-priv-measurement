import os
import sys
import pandas as pd
from dotenv import load_dotenv
from collections import defaultdict
from sqlalchemy import create_engine

# Allow running this file directly from analysis/tracking/.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from analysis.utils.urls import extract_domain_from_url
from analysis.utils.sessions import filter_global_to_common_etlds
from analysis.utils.similarity import build_value_sets_by_group, compute_pairwise_similarity_rows, compute_similarity_matrices
from analysis.tracking.country_specific import run_country_specific_outputs


# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = "reports_tracking"
SIMILARITY_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "similarity")
COUNTRY_SPECIFIC_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "specific_to_each_country")
TOP_TRACKERS_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "top_trackers")
D3_COUNTRY_DATASETS_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "d3_country_datasets")
GROUP_COLS = ["etld", "category", "location"]
countries = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
TOP_K = 10

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SIMILARITY_OUTPUT_DIR, exist_ok=True)
os.makedirs(COUNTRY_SPECIFIC_OUTPUT_DIR, exist_ok=True)
os.makedirs(TOP_TRACKERS_OUTPUT_DIR, exist_ok=True)
os.makedirs(D3_COUNTRY_DATASETS_OUTPUT_DIR, exist_ok=True)



# ============================================================
# DATA LOADING
# ============================================================

# Load sessions and third_party tracking requests from the database.
def load_tables():
    load_dotenv()
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}/"
        f"{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )

    sessions = pd.read_sql("""
        SELECT
            id AS session_id,
            etld,
            url,
            category,
            location,
            access_id
        FROM crawl_sessions
        WHERE 
          location NOT LIKE '%%VPN'
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
          AND
          (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
    """, engine)

    requests = pd.read_sql("""
        SELECT
            session_id,
            request_url
        FROM requests
        WHERE is_tracker = 1
        AND etld_request_url_rel = 'cross-site'
        AND session_id IN (
            SELECT id
            FROM crawl_sessions
            WHERE 
                 location NOT LIKE '%%VPN'
                 AND (is_cloudflare IS NULL OR is_cloudflare = 0)
                 AND
                 (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        )
    """, engine)

    return sessions, requests


# Build a site-level tracker-presence table.
# Output format: one row per unique (etld, category, location, tracker_domain)
def build_site_tracking_table(sessions, requests):
    """
    One row = (etld, category, location, tracker_domain)
    """

    df = requests.merge(
        sessions,
        on="session_id",
        how="inner",
    )

    df["tracker_domain"] = df["request_url"].apply(extract_domain_from_url)
    df = df.dropna(subset=["tracker_domain"])

    # Reduce to site-level presence
    df = df.drop_duplicates(
        subset=["etld", "category", "location", "tracker_domain"]
    )

    return df[["etld", "category", "location", "tracker_domain"]]

# Compute share of sites with at least one third-party tracker.
def compute_tracking_prevalence(sessions, site_tracking):
    """
    Percentage of etlds with at least one third-party tracker.
    """

    # List all distinct sites for each category and country (denominators)
    all_etlds = (
        sessions[["etld", "category", "location"]]
        .drop_duplicates()
    )

    # Mark sites that made at least one third-party tracking request (numerators)
    tracked_etlds = (
        site_tracking[["etld", "category", "location"]]
        .drop_duplicates()
        .assign(has_tracking=1)
    )

    # Join all sites with tracked sites and fill missing with zero
    merged = all_etlds.merge(
        tracked_etlds,
        on=["etld", "category", "location"],
        how="left",
    ).fillna({"has_tracking": 0})

    # Compute prevalence (%) by category and country.
    prevalence = (
        merged.groupby(["category", "location"])["has_tracking"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
    )

    # Add combined overall row across D1/D2/D3 (exclude D2_EQUIVALENT).
    merged_d1_d2_d3 = merged[
        merged["category"].isin(["global", "country_coded", "country_specific"])
    ].copy()
    overall_prevalence = (
        merged_d1_d2_d3.groupby("location")["has_tracking"]
        .mean()
        .multiply(100)
        .round(2)
    )
    overall_row = pd.DataFrame(
        [overall_prevalence],
        index=["overall (D1/D2/D3)"],
    )
    prevalence = pd.concat([prevalence, overall_row], axis=0)

    return prevalence

# Compute average number of distinct trackers per site.
def compute_avg_trackers_per_site(site_tracking):
    """
    Average number of distinct third-party trackers per etld,
    per (category, location).
    """

    # Count distinct tracker domains for each individual site
    per_site = (
        site_tracking
        .groupby(["category", "location", "etld"])["tracker_domain"]
        .nunique()
        .reset_index(name="num_trackers")
    )

    # Compute the average number of trackers per site for each category-country pair
    avg = (
        per_site
        .groupby(["category", "location"])["num_trackers"]
        .mean()
        .unstack()
        .round(2)
    )

    # Add combined overall row across D1/D2/D3 (exclude D2_EQUIVALENT).
    site_tracking_d1_d2_d3 = site_tracking[
        site_tracking["category"].isin(["global", "country_coded", "country_specific"])
    ].copy()
    overall_avg = (
        site_tracking_d1_d2_d3
        .groupby(["location", "etld"])["tracker_domain"]
        .nunique()
        .groupby("location")
        .mean()
        .round(2)
    )
    overall_row = pd.DataFrame(
        [overall_avg],
        index=["overall (D1/D2/D3)"],
    )
    avg = pd.concat([avg, overall_row], axis=0)

    return avg


# Compute total distinct trackers observed per country and category.
def compute_total_distinct_trackers(site_tracking):
    """
    Total number of distinct third-party tracking domains
    per (category, location).
    """

    total = (
        site_tracking
        .groupby(["category", "location"])["tracker_domain"]
        .nunique()
        .unstack()
        .fillna(0)
        .astype(int)
    )

    # Add combined overall row across D1/D2/D3 (exclude D2_EQUIVALENT).
    site_tracking_d1_d2_d3 = site_tracking[
        site_tracking["category"].isin(["global", "country_coded", "country_specific"])
    ].copy()
    overall_total = (
        site_tracking_d1_d2_d3
        .groupby("location")["tracker_domain"]
        .nunique()
    )
    overall_row = pd.DataFrame(
        [overall_total],
        index=["overall (D1/D2/D3)"],
    )
    total = pd.concat([total, overall_row], axis=0).fillna(0).astype(int)

    return total


# Write combined country+category summary table.
def write_tracking_summary(prevalence, avg_trackers, total_trackers):
    categories = [
        "global",
        "country_coded",
        "country_specific",
        "D2_EQUIVALENT",
        "overall (D1/D2/D3)",
    ]

    panel_map = {
        "tracking_prevalence_pct": prevalence,
        "avg_num_trackers_per_site": avg_trackers,
        "total_num_distinct_trackers": total_trackers,
    }

    # Build one row per metric x category (including overall).
    summary_rows = []
    for metric, metric_df in panel_map.items():
        for category in categories:
            row = {"metric": metric, "category": category}
            for country in countries:
                value = None
                if category in metric_df.index and country in metric_df.columns:
                    value = metric_df.loc[category, country]
                row[country] = value
            summary_rows.append(row)

    # Write the final summary table to CSV.
    pd.DataFrame(summary_rows).to_csv(
        f"{OUTPUT_DIR}/tracking_summary.csv",
        index=False,
    )


# Write overall and per-catalog similarity outputs.
def _format_pairwise_rows_for_output(pair_rows):
    # Convert generic set-similarity keys into explicit country-pair columns.
    return [
        {
            "country_a": row["group_a"],
            "country_b": row["group_b"],
            "shared_count": row["shared_count"],
            "union_count": row["union_count"],
            "jaccard_pct": row["jaccard_pct"],
            "overlap_coefficient_pct": row["overlap_coefficient_pct"],
        }
        for row in pair_rows
    ]

# Write pairwise similarity rows and all similarity matrices for a single labeled subset
def _write_similarity_outputs_for_label(value_sets, label):
    pair_rows = compute_pairwise_similarity_rows(value_sets, countries)
    pair_rows = _format_pairwise_rows_for_output(pair_rows)
    pd.DataFrame(pair_rows).to_csv(
        f"{SIMILARITY_OUTPUT_DIR}/pairwise_similarity_jaccard_pct_and_overlap_coefficient_{label}.csv",
        index=False,
    )

    # Build and write intersection, Jaccard, and overlap coefficient matrices.
    intersection_count_matrix, jaccard_matrix, overlap_coefficient_matrix = compute_similarity_matrices(
        value_sets,
        countries,
        index_name="country",
    )
    intersection_count_matrix.to_csv(
        f"{SIMILARITY_OUTPUT_DIR}/intersection_count_matrix_{label}.csv"
    )
    jaccard_matrix.to_csv(
        f"{SIMILARITY_OUTPUT_DIR}/jaccard_pct_matrix_{label}.csv"
    )
    overlap_coefficient_matrix.to_csv(f"{SIMILARITY_OUTPUT_DIR}/overlap_coefficient_matrix_{label}.csv")

# Write Jaccard and overlap-coefficient similarity outputs for sets of trackers across catalogs and countries
def write_similarity_outputs(site_tracking):
    # Define the subsets: D1, D2, D3, D2_EQUIVALENT, and overall (D1/D2/D3).
    labeled_subsets = {
        "overall_d1_d2_d3": site_tracking[site_tracking["category"].isin(["global", "country_coded", "country_specific"])].copy(),
        "d1": site_tracking[site_tracking["category"] == "global"].copy(),
        "d2": site_tracking[site_tracking["category"] == "country_coded"].copy(),
        "d3": site_tracking[site_tracking["category"] == "country_specific"].copy(),
        "d2_equivalent": site_tracking[site_tracking["category"] == "D2_EQUIVALENT"].copy(),
    }

    for label, subset_df in labeled_subsets.items():
        # Example output: {"ALGERIA": {"google-analytics.com", "doubleclick.net"}, ...}
        value_sets = build_value_sets_by_group(
            subset_df,
            group_col="location",
            value_col="tracker_domain",
        )
        _write_similarity_outputs_for_label(value_sets, label)

# Build top tracker list and matrix for a given data subset.
def compute_top_trackers_for_subset(
    sessions,
    site_tracking,
):

    denominators = (
        sessions[["location", "etld"]]
        .drop_duplicates()
        .groupby("location")
        .size()
        .to_dict()
    )

    presence = defaultdict(set)
    for _, row in site_tracking.iterrows():
        presence[(row["location"], row["tracker_domain"])].add(row["etld"])

    rows = []
    for (loc, dom), etlds in presence.items():
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue
        rows.append({
            "location": loc,
            "tracker": dom,
            "percentage": round(len(etlds) / denom * 100, 2),
        })

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        empty_list = pd.DataFrame(columns=["Rank"] + countries)
        empty_matrix = pd.DataFrame(columns=["Tracker"] + countries)
        return empty_list, empty_matrix

    ranked = ranked.sort_values(
        ["location", "percentage", "tracker"],
        ascending=[True, False, True],
    )
    ranked["rank"] = ranked.groupby("location").cumcount() + 1

    top_ranked = ranked[ranked["rank"] <= TOP_K].copy()
    top_ranked["entry"] = top_ranked.apply(
        lambda r: f"{r['tracker']} ({r['percentage']:.2f}%)",
        axis=1,
    )

    list_df = (
        top_ranked
        .pivot(index="rank", columns="location", values="entry")
        .reindex(index=range(1, TOP_K + 1), columns=countries)
        .reset_index()
        .rename(columns={"rank": "Rank"})
    )

    tracker_rank = pd.concat(
        [
            ranked.groupby("tracker")["location"].nunique().rename("n_countries"),
            ranked.groupby("tracker")["percentage"].mean().rename("avg_pct"),
        ],
        axis=1,
    ).sort_values(["n_countries", "avg_pct"], ascending=[False, False])
    top_trackers = tracker_rank.head(TOP_K).index.tolist()

    matrix_df = (
        ranked[ranked["tracker"].isin(top_trackers)]
        .pivot_table(index="tracker", columns="location", values="percentage", aggfunc="first")
        .reindex(index=top_trackers, columns=countries)
        .reset_index()
        .rename(columns={"tracker": "Tracker"})
    )

    return list_df, matrix_df

# Write top tracker outputs for overall and each bucket.
def write_top_tracker_outputs(
    sessions,
    site_tracking,
):
    bucket_map = {
        "overall_d1_d2_d3": (
            sessions[sessions["category"].isin(["global", "country_coded", "country_specific"])].copy(),
            site_tracking[site_tracking["category"].isin(["global", "country_coded", "country_specific"])].copy(),
        ),
        "d1": (
            sessions[sessions["category"] == "global"].copy(),
            site_tracking[site_tracking["category"] == "global"].copy(),
        ),
        "d2": (
            sessions[sessions["category"] == "country_coded"].copy(),
            site_tracking[site_tracking["category"] == "country_coded"].copy(),
        ),
        "d3": (
            sessions[sessions["category"] == "country_specific"].copy(),
            site_tracking[site_tracking["category"] == "country_specific"].copy(),
        ),
        "d2_equivalent": (
            sessions[sessions["category"] == "D2_EQUIVALENT"].copy(),
            site_tracking[site_tracking["category"] == "D2_EQUIVALENT"].copy(),
        ),
    }

    for bucket_name, (bucket_sessions, bucket_tracking) in bucket_map.items():
        list_df, matrix_df = compute_top_trackers_for_subset(
            bucket_sessions,
            bucket_tracking,
        )
        list_df.to_csv(
            f"{TOP_TRACKERS_OUTPUT_DIR}/top_trackers_{bucket_name}.csv",
            index=False,
        )
        matrix_df.to_csv(
            f"{TOP_TRACKERS_OUTPUT_DIR}/top_trackers_matrix_{bucket_name}.csv",
            index=False,
        )


def write_d3_country_datasets(sessions, requests):
    """Write one D3 CSV per country with unique tracker counts per (location, site)."""
    d3_sessions = sessions[sessions["category"] == "country_specific"].copy()
    if d3_sessions.empty:
        for country in countries:
            out_path = os.path.join(D3_COUNTRY_DATASETS_OUTPUT_DIR, f"d3_{country}.csv")
            pd.DataFrame(columns=["location", "site", "n_trackers"]).to_csv(out_path, index=False)
        return

    d3_keys = (
        d3_sessions[["session_id", "location", "etld"]]
        .drop_duplicates()
        .rename(columns={"etld": "site"})
    )

    # Build tracker-domain lists per key by merging requests across all corresponding sessions.
    d3_trackers = requests.merge(d3_keys, on="session_id", how="inner")
    d3_trackers["tracker_domain"] = d3_trackers["request_url"].apply(extract_domain_from_url)
    d3_trackers = d3_trackers.dropna(subset=["tracker_domain"])
    d3_trackers = d3_trackers[d3_trackers["tracker_domain"].astype(str).str.len() > 0]

    grouped = (
        d3_trackers
        .groupby(["location", "site"], as_index=False)["tracker_domain"]
        .agg(lambda vals: sorted(set(vals)))
    )
    grouped["n_trackers"] = grouped["tracker_domain"].apply(len)
    grouped = grouped[["location", "site", "n_trackers"]]

    # Keep zero rows for retained D3 location/site combinations without tracker observations.
    all_keys = d3_keys[["location", "site"]].drop_duplicates()
    d3_counts = all_keys.merge(
        grouped,
        on=["location", "site"],
        how="left",
    )
    d3_counts["n_trackers"] = d3_counts["n_trackers"].fillna(0).astype(int)

    for country in countries:
        country_df = (
            d3_counts[d3_counts["location"] == country]
            .sort_values(["site"])
            .reset_index(drop=True)
        )
        out_path = os.path.join(D3_COUNTRY_DATASETS_OUTPUT_DIR, f"d3_{country}.csv")
        country_df.to_csv(out_path, index=False)



# Run the full tracking analysis pipeline.
def main():

    #reading crawl_sessions + third-party tracking requests
    sessions, requests = load_tables()

    # Build a site-level tracker presence table.
    # One row per unique (etld, category, location, tracker_domain)
    site_tracking = build_site_tracking_table(
        sessions, requests
    )

    # Restrict global data to sites present in all five countries.
    sessions, site_tracking = filter_global_to_common_etlds(
        sessions, site_tracking
    )

    # Aggregate base tables used by summary writers.
    prevalence = compute_tracking_prevalence(sessions, site_tracking)
    avg_trackers = compute_avg_trackers_per_site(site_tracking)
    total_trackers = compute_total_distinct_trackers(site_tracking)

    # Run each output writer separately to keep responsibilities small.
    write_tracking_summary(
        prevalence,
        avg_trackers,
        total_trackers,
    )
    
    write_similarity_outputs(site_tracking)
    run_country_specific_outputs(
        sessions,
        site_tracking,
        COUNTRY_SPECIFIC_OUTPUT_DIR,
        countries,
        TOP_K,
    )
    write_top_tracker_outputs(sessions, site_tracking)
    write_d3_country_datasets(sessions, requests)

if __name__ == "__main__":
    main()