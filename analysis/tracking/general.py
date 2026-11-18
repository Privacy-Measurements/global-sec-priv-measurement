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
from analysis.tracking.country_specific import compute_country_specific_top_trackers


OUTPUT_DIR = "reports_tracking"
GROUP_COLS = ["etld", "category", "location"]
countries = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
TOP_K = 10

os.makedirs(OUTPUT_DIR, exist_ok=True)




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
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
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
                 AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        )
    """, engine)

    return sessions, requests


# Output format: one row per unique (etld, category, location, tracker_domain)
def build_site_tracking_table(sessions, requests):
    """
    For each site (etld, category, location):
      1. Group by access_id
      2. For each access_id, determine which trackers it contacts
      3. Keep only trackers that appear in ALL access_ids
    
    Output: One row per (etld, category, location, tracker_domain)
            where tracker_domain appears consistently across ALL access_ids for that site
    """
    
    df = requests.merge(
        sessions,
        on="session_id",
        how="inner",
    )

    df["tracker_domain"] = df["request_url"].apply(extract_domain_from_url)
    df = df.dropna(subset=["tracker_domain"])

    rows = []
    
    for (etld, category, location), site_data in df.groupby(["etld", "category", "location"]):
        access_ids = site_data["access_id"].unique()
        
        # For each access_id, get the set of trackers it contacts
        trackers_by_access_id = {}
        for access_id in access_ids:
            access_data = site_data[site_data["access_id"] == access_id]
            trackers_by_access_id[access_id] = set(access_data["tracker_domain"].unique())
        
        # Find intersection: trackers present in ALL access_ids
        if not trackers_by_access_id:
            continue
        
        tracker_sets = list(trackers_by_access_id.values())
        if all(tracker_sets):  # All access_ids have at least one tracker
            common_trackers = set.intersection(*tracker_sets)
            for tracker in common_trackers:
                rows.append({
                    "etld": etld,
                    "category": category,
                    "location": location,
                    "tracker_domain": tracker,
                })
    
    if not rows:
        return pd.DataFrame(columns=["etld", "category", "location", "tracker_domain"])
    
    result_df = pd.DataFrame(rows)
    result_df = result_df[["etld", "category", "location", "tracker_domain"]]
    result_df.to_csv(os.path.join(OUTPUT_DIR, "site_level_tracking.csv"), index=False)
    return result_df

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


def write_tracking_summary(prevalence, avg_trackers, total_trackers):
    categories = [
        "global",
        "country_coded",
        "country_specific",
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


# Build top tracker list and matrix for a given data subset.
def compute_top_trackers_for_subset(sessions, site_tracking):

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

# Write only the overall top-tracker matrix for the D1/D2/D3 aggregate.
def write_top_tracker_outputs(sessions, site_tracking):
    subset_sessions = sessions[sessions["category"].isin(["global", "country_coded", "country_specific"])].copy()
    subset_tracking = site_tracking[site_tracking["category"].isin(["global", "country_coded", "country_specific"])].copy()

    _, matrix_df = compute_top_trackers_for_subset(
        subset_sessions,
        subset_tracking,
    )
    matrix_df.to_csv(
        f"{OUTPUT_DIR}/top_trackers_d1_d2_d3.csv",
        index=False,
    )


# Run the full tracking analysis pipeline.
def main():

    sessions, requests = load_tables()

    # Build a site-level tracker presence table.
    # One row per unique (etld, category, location, tracker_domain)
    site_tracking = build_site_tracking_table(sessions, requests)
    sessions, site_tracking = filter_global_to_common_etlds(sessions, site_tracking)

    # Aggregate base tables used by summary writers.
    prevalence = compute_tracking_prevalence(sessions, site_tracking)
    avg_trackers = compute_avg_trackers_per_site(site_tracking)
    total_trackers = compute_total_distinct_trackers(site_tracking)

    # Run each output writer separately to keep responsibilities small.
    write_tracking_summary(prevalence, avg_trackers, total_trackers)

    overall_sessions = sessions[sessions["category"].isin(["global", "country_coded", "country_specific"])].copy()
    overall_tracking = site_tracking[site_tracking["category"].isin(["global", "country_coded", "country_specific"])].copy()

    overall_top_trackers = compute_country_specific_top_trackers(overall_sessions, overall_tracking, countries, None)

    overall_top_trackers.to_csv(
        os.path.join(OUTPUT_DIR, "top_country_specific_trackers_d1_d2_d3.csv"),
        index=False,
    )
    write_top_tracker_outputs(sessions, site_tracking)

if __name__ == "__main__":
    main()