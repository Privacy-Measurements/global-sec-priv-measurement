import os, sys, itertools
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import urlparse

# Allow running this file directly from analysis/tracking/.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from analysis.utils.urls import extract_domain_from_url, normalize_url



OUTPUT_DIR = "reports_D1"
COUNTRIES = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
MIN_REQUESTS_PER_SESSION = 10

os.makedirs(OUTPUT_DIR, exist_ok=True)




def jaccard_pct(set_a, set_b):
    # Jaccard = intersection / union, expressed as percentage.
    union_size = len(set_a | set_b)
    if union_size == 0:
        return 0.0
    return round((len(set_a & set_b) / union_size) * 100, 2)

def overlap_pct(set_a, set_b):
    # Overlap coefficient = intersection / min(|A|, |B|), expressed as percentage.
    min_size = min(len(set_a), len(set_b))
    if min_size == 0:
        return 0.0
    return round((len(set_a & set_b) / min_size) * 100, 2)

def load_tables():
    # Load D1/global sessions and requests, then split requests into all vs tracking-only.
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
            access_id
        FROM crawl_sessions
        WHERE category = 'global'
          AND location NOT LIKE '%%VPN'
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        """,
        engine,
    )

    # All requests are needed for the min-10-requests filter.
    requests_all = pd.read_sql(
        """
        SELECT
            id,
            session_id,
            parent_id,
            request_url,
            is_tracker,
            etld_request_url_rel
        FROM requests
        WHERE session_id IN (
            SELECT id
            FROM crawl_sessions
            WHERE category = 'global'
              AND location NOT LIKE '%%VPN'
              AND (is_cloudflare IS NULL OR is_cloudflare = 0)
              AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        )
        """,
        engine,
    )

    # Tracker-only cross-site requests are the input for tracker-set comparisons.
    tracker_requests = requests_all[
        (requests_all["is_tracker"] == 1)
        & (requests_all["etld_request_url_rel"] == "cross-site")
    ][["session_id", "id", "parent_id", "request_url"]].copy()
    tracker_requests = tracker_requests.rename(columns={"id": "request_id"})

    # Normalize URLs so the same page can be matched across countries.
    sessions["normalized_url"] = sessions["url"].apply(normalize_url)
    return sessions, requests_all[["session_id", "request_url"]], tracker_requests

# UNIVERSE FILTERING
def keep_sessions_with_min_requests(sessions, all_requests):
    # Step 2: keep only sessions with at least N requests (all requests, not only trackers).
    request_counts = all_requests.groupby("session_id").size()
    valid_session_ids = set(request_counts[request_counts >= MIN_REQUESTS_PER_SESSION].index)
    return sessions[sessions["session_id"].isin(valid_session_ids)].copy()

def keep_etlds_in_all_countries(sessions):
    # Build etld -> set(countries) and keep exact five-country coverage.
    etld_country_sets = sessions.groupby("etld")["location"].agg(set)
    keep_etlds = {etld for etld, locs in etld_country_sets.items() if locs == set(COUNTRIES)}
    return sessions[sessions["etld"].isin(keep_etlds)].copy()

def keep_etlds_with_non_empty_url_intersection(sessions):
    # Keep the etld only when this intersection is non-empty, and keep only intersection URLs.
    keep_pairs = set()

    for etld, etld_df in sessions.groupby("etld"):
        per_country_url_sets = []
        for country in COUNTRIES:
            country_urls = set(
                etld_df[etld_df["location"] == country]["normalized_url"].tolist()
            )
            per_country_url_sets.append(country_urls)

        common_urls = set.intersection(*per_country_url_sets) if per_country_url_sets else set()
        if not common_urls:
            continue

        for url in common_urls:
            keep_pairs.add((etld, url))

    keep_mask = sessions.apply(
        lambda row: (row["etld"], row["normalized_url"]) in keep_pairs,
        axis=1,
    )
    return sessions[keep_mask].copy()

def build_strict_d1_universe(sessions, all_requests, tracker_requests):
    # keep sessions with at least MIN_REQUESTS_PER_SESSION total requests.
    s = keep_sessions_with_min_requests(sessions, all_requests)
    # keep only etlds that are observed in all five countries.
    s = keep_etlds_in_all_countries(s)
    # keep only the URL intersection across countries for each etld.
    # If an etld has empty intersection, it is dropped.
    s = keep_etlds_with_non_empty_url_intersection(s)

    # Keep only tracker requests belonging to surviving sessions.
    kept_session_ids = set(s["session_id"])
    tr_req = tracker_requests[tracker_requests["session_id"].isin(kept_session_ids)].copy()

    return s, tr_req

# TRACKER SET CONSTRUCTION
def build_site_tracking(sessions, tracker_requests):
    # Merge trackers with surviving sessions, 
    # Then keep only trackers that are present across all access_ids for a site-country pair.
    df = tracker_requests.merge(
        sessions[["session_id", "etld", "location", "access_id", "normalized_url"]],
        on="session_id",
        how="inner",
    )

    df["tracker_domain"] = df["request_url"].apply(extract_domain_from_url)
    df = df.dropna(subset=["tracker_domain"])

    rows = []

    for (etld, location), site_data in df.groupby(["etld", "location"]):
        access_ids = site_data["access_id"].unique()
        trackers_by_access_id = {}

        for access_id in access_ids:
            access_data = site_data[site_data["access_id"] == access_id]
            trackers_by_access_id[access_id] = set(access_data["tracker_domain"].unique())

        if not trackers_by_access_id:
            continue

        tracker_sets = list(trackers_by_access_id.values())
        if not all(tracker_sets):
            continue

        common_trackers = set.intersection(*tracker_sets)
        for tracker in common_trackers:
            rows.append({
                "etld": etld,
                "location": location,
                "tracker_domain": tracker,
            })

    if not rows:
        return pd.DataFrame(columns=["etld", "location", "tracker_domain"])

    result_df = pd.DataFrame(rows)
    return result_df[["etld", "location", "tracker_domain"]]

def build_etld_country_tracker_sets(sessions, site_tracking):
    # Initialize all etld-country pairs with empty sets to keep zero-tracker cases visible.
    etlds = sorted(sessions["etld"].unique())
    tracker_sets = {
        etld: {country: set() for country in COUNTRIES}
        for etld in etlds
    }

    # Fill sets from observed tracker domains per etld-country.
    grouped = site_tracking.groupby(["etld", "location"])["tracker_domain"].agg(set)
    for (etld, location), dom_set in grouped.items():
        if etld in tracker_sets and location in tracker_sets[etld]:
            tracker_sets[etld][location] = set(dom_set)

    return tracker_sets

# EXPORTS
def build_etld_pairwise_similarity(tracker_sets):
    # Compute pairwise tracker-set similarity for each etld.
    rows = []
    for etld, per_country in tracker_sets.items():
        for country_a, country_b in itertools.combinations(COUNTRIES, 2):
            set_a = per_country[country_a]
            set_b = per_country[country_b]
            shared = len(set_a & set_b)
            union = len(set_a | set_b)

            # One row per (etld, country_a, country_b) with set-level similarity stats.
            rows.append(
                {
                    "etld": etld,
                    "country_a": country_a,
                    "country_b": country_b,
                    "shared_count": shared,
                    "union_count": union,
                    "jaccard_pct": jaccard_pct(set_a, set_b),
                    "overlap_coefficient_pct": overlap_pct(set_a, set_b),
                    "exact_same_set": set_a == set_b,
                    "only_in_a_count": len(set_a - set_b),
                    "only_in_b_count": len(set_b - set_a),
                }
            )

    return pd.DataFrame(rows)

def export_country_pair_summary(pairwise_etld_df):
    # Aggregate etld-level pairwise metrics into country-vs-country summaries.
    summary = (
        pairwise_etld_df
        .groupby(["country_a", "country_b"])
        .agg(
            avg_jaccard_pct=("jaccard_pct", "mean"),
            median_jaccard_pct=("jaccard_pct", "median"),
            avg_overlap_pct=("overlap_coefficient_pct", "mean"),
            pct_exact_same_set=("exact_same_set", lambda vals: round(vals.mean() * 100, 2)),
            pct_zero_overlap=("shared_count", lambda vals: round((vals == 0).mean() * 100, 2)),
            avg_only_in_a_count=("only_in_a_count", "mean"),
            avg_only_in_b_count=("only_in_b_count", "mean"),
        )
        .reset_index()
    )

    # Keep two decimals for readability of averaged metrics.
    for col in [
        "avg_jaccard_pct",
        "median_jaccard_pct",
        "avg_overlap_pct",
        "avg_only_in_a_count",
        "avg_only_in_b_count",
    ]:
        summary[col] = summary[col].round(2)

    summary.to_csv(f"{OUTPUT_DIR}/country_pair_similarity_summary.csv", index=False)

def export_etld_country_pair_tracker_differences(tracker_sets):
    # Save per-etld tracker sets and set differences for every ordered country pair.
    rows = []

    for etld, per_country in tracker_sets.items():
        for country_a, country_b in itertools.permutations(COUNTRIES, 2):
            trackers_in_a = per_country[country_a]
            trackers_in_b = per_country[country_b]
            only_in_a = trackers_in_a - trackers_in_b
            only_in_b = trackers_in_b - trackers_in_a

            rows.append(
                {
                    "etld": etld,
                    "country_a": country_a,
                    "country_b": country_b,
                    "n_trackers_in_a": len(trackers_in_a),
                    "n_trackers_in_b": len(trackers_in_b),
                    "n_shared_trackers": len(trackers_in_a & trackers_in_b),
                    "n_only_in_a": len(only_in_a),
                    "n_only_in_b": len(only_in_b),
                    "trackers_in_a": "|".join(sorted(trackers_in_a)),
                    "trackers_in_b": "|".join(sorted(trackers_in_b)),
                    "trackers_only_in_a": "|".join(sorted(only_in_a)),
                    "trackers_only_in_b": "|".join(sorted(only_in_b)),
                }
            )

    detail_df = pd.DataFrame(rows)
    detail_df.to_csv(f"{OUTPUT_DIR}/etld_country_pair_tracker_differences.csv", index=False)
    return detail_df

def export_country_pair_tracker_asymmetry(etld_country_pair_diff_df):
    # For each ordered country pair, count tracker domains that appear in a but not in b.
    etld_count = etld_country_pair_diff_df["etld"].nunique()
    rows = []

    for (country_a, country_b), pair_df in etld_country_pair_diff_df.groupby(["country_a", "country_b"]):
        domain_counts = {}

        for trackers_only_in_a in pair_df["trackers_only_in_a"].tolist():
            if not trackers_only_in_a:
                continue
            only_in_a = trackers_only_in_a.split("|")
            for tracker_domain in only_in_a:
                domain_counts[tracker_domain] = domain_counts.get(tracker_domain, 0) + 1

        for tracker_domain, n_etlds in sorted(
            domain_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:10]:
            rows.append(
                {
                    "country_present": country_a,
                    "country_missing": country_b,
                    "tracker_domain": tracker_domain,
                    "n_etlds_present_only": n_etlds,
                    "pct_etlds_present_only": round((n_etlds / etld_count) * 100, 2) if etld_count else 0.0,
                }
            )

    pd.DataFrame(rows).to_csv(f"{OUTPUT_DIR}/country_pair_tracker_asymmetry.csv", index=False)


def main():
    # Load raw D1/global sessions and requests.
    sessions, all_requests, tracker_requests = load_tables()

    # Build strict comparable universe and keep only corresponding requests.
    strict_sessions, strict_tracker_requests = build_strict_d1_universe(sessions, all_requests, tracker_requests)

    # Build per-etld/per-country tracker sets from tracker requests.
    site_tracking = build_site_tracking(strict_sessions, strict_tracker_requests)
    tracker_sets = build_etld_country_tracker_sets(strict_sessions, site_tracking)

    # Keep only the two requested outputs.
    etld_pairwise = build_etld_pairwise_similarity(tracker_sets)
    export_country_pair_summary(etld_pairwise)
    etld_country_pair_diff_df = export_etld_country_pair_tracker_differences(tracker_sets)
    export_country_pair_tracker_asymmetry(etld_country_pair_diff_df)



if __name__ == "__main__":
    main()
