import os, sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Allow running this file directly from analysis/tracking/.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from analysis.tracking.d2_session_level_deviation import export_d2_session_level_deviation_analysis
from analysis.utils.urls import extract_domain_from_url


OUTPUT_DIR = "reports_D2"
MIN_REQUESTS_PER_SESSION = 10
COUNTRY_CODE_TO_NAME = {
    "DZ": "ALGERIA",
    "DE": "GERMANY",
    "IN": "INDIA",
    "AE": "UAE",
    "US": "USA",
}
D2_MAPPING_DIR = os.path.join(
    REPO_ROOT,
    "site_catalogs_construction",
    "catalogs",
    "D2_to_global",
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load the D2 -> equivalent mapping catalogs
def load_pair_mapping():
    frames = []
    for code, country in COUNTRY_CODE_TO_NAME.items():
        # Each country file gives a D2 site and the site that should be used as its same-country comparison target.
        path = os.path.join(D2_MAPPING_DIR, f"d2_to_global_{code}.csv")
        mapping = pd.read_csv(path)
        mapping["country"] = country
        # If the equivalent is part of D1, it must exist in the DB as global.
        # Otherwise it must exist as D2_EQUIVALENT.
        mapping["equivalent_category"] = mapping["in_d1"].apply(
            lambda value: "global" if str(value).strip().lower() == "true" else "D2_EQUIVALENT"
        )
        mapping = mapping.rename(
            columns={
                "d2_site": "d2_etld",
                "global_site": "equivalent_etld",
            }
        )
        frames.append(mapping[["country", "d2_etld", "equivalent_etld", "in_d1", "equivalent_category"]])

    out = pd.concat(frames, ignore_index=True)
    # Drop duplicates in case the source catalogs contain repeated rows.
    out = out.drop_duplicates(["country", "d2_etld", "equivalent_etld", "equivalent_category"])
    return out


def load_tables():
    # Load D2, global, and D2_EQUIVALENT sessions plus all requests needed by the analysis.
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
            location
        FROM crawl_sessions
        WHERE category IN ('country_coded', 'global', 'D2_EQUIVALENT')
          AND location NOT LIKE '%%VPN'
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        """,
        engine,
    )

    # Keep all requests because the min-10-requests filter uses total request volume, not only trackers.
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
            WHERE category IN ('country_coded', 'global', 'D2_EQUIVALENT')
              AND location NOT LIKE '%%VPN'
              AND (is_cloudflare IS NULL OR is_cloudflare = 0)
              AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        )
        """,
        engine,
    )

    # Tracker-only cross-site requests are the inputs for tracker-presence comparisons.
    tracker_requests = requests_all[
        (requests_all["is_tracker"] == 1)
        & (requests_all["etld_request_url_rel"] == "cross-site")
    ][["session_id", "id", "parent_id", "request_url"]].copy()
    tracker_requests = tracker_requests.rename(columns={"id": "request_id"})

    return sessions, requests_all[["session_id", "request_url"]], tracker_requests


def keep_sessions_with_min_requests(sessions, all_requests, tracker_requests):
    # Keep only sessions with at least MIN_REQUESTS_PER_SESSION total requests.
    # Apply the same filter consistently to sessions, all requests, and tracker-only requests.
    request_counts = all_requests.groupby("session_id").size()
    valid_session_ids = set(request_counts[request_counts >= MIN_REQUESTS_PER_SESSION].index)

    filtered_sessions = sessions[sessions["session_id"].isin(valid_session_ids)].copy()
    filtered_requests = all_requests[all_requests["session_id"].isin(valid_session_ids)].copy()
    filtered_tracker_requests = tracker_requests[tracker_requests["session_id"].isin(valid_session_ids)].copy()
    return filtered_sessions, filtered_requests, filtered_tracker_requests

# Keep only D2 sites whose same-country equivalent also exists in the database with the expected category
def build_pair_universe(sessions, mapping):
    session_sites = sessions[["session_id", "location", "etld", "category"]].drop_duplicates()

    # Count available D2 sessions for each (country, d2_etld).
    d2_sites = (
        session_sites[session_sites["category"] == "country_coded"]
        .groupby(["location", "etld"])["session_id"]
        .nunique()
        .reset_index(name="n_d2_sessions")
        .rename(columns={"location": "country", "etld": "d2_etld"})
    )

    # Count available equivalent sessions for each (country, equivalent_etld, expected_category).
    equivalent_sites = (
        session_sites[session_sites["category"].isin(["global", "D2_EQUIVALENT"])]
        .groupby(["location", "etld", "category"])["session_id"]
        .nunique()
        .reset_index(name="n_equivalent_sessions")
        .rename(
            columns={
                "location": "country",
                "etld": "equivalent_etld",
                "category": "equivalent_category",
            }
        )
    )

    # Inner joins enforce that both sides of the pair exist after DB filtering.
    pair_universe = mapping.merge(
        d2_sites,
        on=["country", "d2_etld"],
        how="inner",
    ).merge(
        equivalent_sites,
        on=["country", "equivalent_etld", "equivalent_category"],
        how="inner",
    )

    pair_universe = pair_universe.drop_duplicates(
        ["country", "d2_etld", "equivalent_etld", "equivalent_category"]
    ).copy()
    # Build a stable pair identifier used across all downstream reports.
    pair_universe["pair_id"] = pair_universe.apply(
        lambda row: f"{row['country']}|{row['d2_etld']}|{row['equivalent_etld']}|{row['equivalent_category']}",
        axis=1,
    )
    return pair_universe

# Attach each surviving session to either the D2 side or the same-country equivalent side of a pair.
# D2 sessions match on the country-coded site itself.
def build_paired_sessions(sessions, pair_universe):

    d2_sessions = sessions[sessions["category"] == "country_coded"].merge(
        pair_universe[["pair_id", "country", "d2_etld", "equivalent_etld", "equivalent_category"]],
        left_on=["location", "etld"],
        right_on=["country", "d2_etld"],
        how="inner",
    )
    d2_sessions["site_side"] = "D2"

    # Equivalent sessions must match on country, etld, and expected category.
    equivalent_sessions = sessions[sessions["category"].isin(["global", "D2_EQUIVALENT"])] .merge(
        pair_universe[["pair_id", "country", "d2_etld", "equivalent_etld", "equivalent_category"]],
        left_on=["location", "etld", "category"],
        right_on=["country", "equivalent_etld", "equivalent_category"],
        how="inner",
    )
    equivalent_sessions["site_side"] = "EQUIVALENT"

    columns = [
        "pair_id",
        "country",
        "d2_etld",
        "equivalent_etld",
        "equivalent_category",
        "site_side",
        "session_id",
        "etld",
        "category",
        "location",
        "url",
    ]
    # Keep all matched sessions because session-level analysis later chooses representative ones.
    paired_sessions = pd.concat(
        [d2_sessions[columns], equivalent_sessions[columns]],
        ignore_index=True,
    )
    return paired_sessions.drop_duplicates(["pair_id", "site_side", "session_id"])


def build_pair_tracking(paired_sessions, tracker_requests):
    # Output format: one row per unique (pair_id, side, tracker_domain).
    # Join tracker requests to the pair metadata needed to compare D2 vs equivalent sides.
    tracking = tracker_requests.merge(
        paired_sessions[["pair_id", "country", "d2_etld", "equivalent_etld", "site_side", "session_id"]],
        on="session_id",
        how="inner",
    )
    tracking["tracker_domain"] = tracking["request_url"].apply(extract_domain_from_url)
    tracking = tracking.dropna(subset=["tracker_domain"])
    # Collapse repeated requests so only site-level tracker presence remains.
    tracking = tracking.drop_duplicates(["pair_id", "site_side", "tracker_domain"])
    return tracking[["pair_id", "country", "d2_etld", "equivalent_etld", "site_side", "tracker_domain"]]


def build_pair_tracker_sets(pair_universe, pair_tracking):
    # Build per-pair tracker sets for the two sides being compared.
    # Initialize both sides with empty sets so zero-tracker pairs remain visible in the outputs.
    tracker_sets = {
        row.pair_id: {"D2": set(), "EQUIVALENT": set()}
        for row in pair_universe.itertuples(index=False)
    }

    # Fill the sets from the observed tracker presence table.
    grouped = pair_tracking.groupby(["pair_id", "site_side"])["tracker_domain"].agg(set)
    for (pair_id, site_side), tracker_domains in grouped.items():
        if pair_id in tracker_sets:
            tracker_sets[pair_id][site_side] = set(tracker_domains)

    return tracker_sets


def save_pair_universe(pair_universe):
    # Save the final comparable D2 universe for debugging and downstream reporting.
    pair_universe.sort_values(["country", "d2_etld"]).to_csv(
        f"{OUTPUT_DIR}/pair_universe.csv",
        index=False,
    )


def export_overall_numbers(pair_universe):
    # Report how many D2 sites per country have an equivalent side available in the database.
    summary = (
        pair_universe.groupby("country")
        .agg(n_pairs=("pair_id", "nunique"))
        .reset_index()
        .sort_values("country")
    )
    summary.to_csv(f"{OUTPUT_DIR}/overall_numbers.csv", index=False)


def export_tracking_prevalence(pair_universe, tracker_sets):
    # Compare site-level tracking prevalence between D2 sites and their same-country equivalents.
    rows = []
    pair_meta = pair_universe.set_index("pair_id")
    for pair_id, side_sets in tracker_sets.items():
        # One row per pair: whether each side shows at least one third-party tracker.
        meta = pair_meta.loc[pair_id]
        rows.append(
            {
                "country": meta["country"],
                "pair_id": pair_id,
                "d2_etld": meta["d2_etld"],
                "equivalent_etld": meta["equivalent_etld"],
                "d2_has_tracking": int(bool(side_sets["D2"])),
                "equivalent_has_tracking": int(bool(side_sets["EQUIVALENT"])),
            }
        )

    pair_level = pd.DataFrame(rows)
    if pair_level.empty:
        # Emit empty files with stable schemas if no comparable pairs survive filtering.
        pd.DataFrame(columns=["country", "pair_id", "d2_etld", "equivalent_etld", "d2_has_tracking", "equivalent_has_tracking"]).to_csv(
            f"{OUTPUT_DIR}/tracking_prevalence_pair_level.csv",
            index=False,
        )
        pd.DataFrame(columns=["country", "n_pairs", "n_d2_with_tracking", "n_equivalent_with_tracking", "pct_d2_with_tracking", "pct_equivalent_with_tracking"]).to_csv(
            f"{OUTPUT_DIR}/tracking_prevalence_summary.csv",
            index=False,
        )
        return pair_level

    pair_level.to_csv(f"{OUTPUT_DIR}/tracking_prevalence_pair_level.csv", index=False)

    # Aggregate pair-level tracking presence into country-level prevalence summaries.
    summary = (
        pair_level.groupby("country")
        .agg(
            n_pairs=("pair_id", "nunique"),
            n_d2_with_tracking=("d2_has_tracking", "sum"),
            n_equivalent_with_tracking=("equivalent_has_tracking", "sum"),
        )
        .reset_index()
        .sort_values("country")
    )
    summary["pct_d2_with_tracking"] = (summary["n_d2_with_tracking"] / summary["n_pairs"] * 100).round(2)
    summary["pct_equivalent_with_tracking"] = (
        summary["n_equivalent_with_tracking"] / summary["n_pairs"] * 100
    ).round(2)
    summary.to_csv(f"{OUTPUT_DIR}/tracking_prevalence_summary.csv", index=False)
    return pair_level


def export_site_tracking_difference_summary(pair_level):
    # Count pairs where one side has tracking and the other side does not.
    if pair_level.empty:
        pd.DataFrame(columns=["country", "n_pairs", "n_d2_only_tracking", "n_equivalent_only_tracking", "pct_d2_only_tracking", "pct_equivalent_only_tracking"]).to_csv(
            f"{OUTPUT_DIR}/site_tracking_difference_summary.csv",
            index=False,
        )
        return

    # Start from the country-level pair counts used as denominators.
    summary = pair_level.groupby("country").agg(
        n_pairs=("pair_id", "nunique"),
        n_d2_only_tracking=("d2_has_tracking", lambda values: 0),
    ).reset_index()

    # Count D2-only tracking cases and equivalent-only tracking cases separately.
    d2_only = (
        pair_level[(pair_level["d2_has_tracking"] == 1) & (pair_level["equivalent_has_tracking"] == 0)]
        .groupby("country")
        .size()
        .rename("n_d2_only_tracking")
        .reset_index()
    )
    equivalent_only = (
        pair_level[(pair_level["d2_has_tracking"] == 0) & (pair_level["equivalent_has_tracking"] == 1)]
        .groupby("country")
        .size()
        .rename("n_equivalent_only_tracking")
        .reset_index()
    )

    # Merge the two asymmetry counts back into one summary table.
    summary = summary.drop(columns=["n_d2_only_tracking"])
    summary = summary.merge(d2_only, on="country", how="left")
    summary = summary.merge(equivalent_only, on="country", how="left")
    summary = summary.fillna({"n_d2_only_tracking": 0, "n_equivalent_only_tracking": 0})
    summary["n_d2_only_tracking"] = summary["n_d2_only_tracking"].astype(int)
    summary["n_equivalent_only_tracking"] = summary["n_equivalent_only_tracking"].astype(int)
    summary["pct_d2_only_tracking"] = (summary["n_d2_only_tracking"] / summary["n_pairs"] * 100).round(2)
    summary["pct_equivalent_only_tracking"] = (summary["n_equivalent_only_tracking"] / summary["n_pairs"] * 100).round(2)
    summary.to_csv(f"{OUTPUT_DIR}/site_tracking_difference_summary.csv", index=False)


def export_tracker_level_differences(pair_universe, tracker_sets):
    # Save per-pair tracker asymmetries and top per-country tracker differences for each side.
    if pair_universe.empty:
        pd.DataFrame(columns=["country", "pair_id", "d2_etld", "equivalent_etld", "n_d2_only_trackers", "n_equivalent_only_trackers", "d2_only_trackers", "equivalent_only_trackers"]).to_csv(
            f"{OUTPUT_DIR}/tracker_level_pair_differences.csv",
            index=False,
        )
        pd.DataFrame(columns=["country", "n_pairs", "n_pairs_with_d2_only_trackers", "pct_pairs_with_d2_only_trackers", "n_pairs_with_equivalent_only_trackers", "pct_pairs_with_equivalent_only_trackers"]).to_csv(
            f"{OUTPUT_DIR}/tracker_level_difference_summary.csv",
            index=False,
        )
        pd.DataFrame(columns=["country", "side", "tracker_domain", "n_pairs_present_only", "pct_pairs_present_only"]).to_csv(
            f"{OUTPUT_DIR}/tracker_level_top10.csv",
            index=False,
        )
        return

    pair_rows = []
    top_rows = []
    summary_rows = []

    for country, country_pairs in pair_universe.groupby("country"):
        d2_only_counter = {}
        equivalent_only_counter = {}
        n_pairs_with_d2_only = 0
        n_pairs_with_equivalent_only = 0

        for row in country_pairs.itertuples(index=False):
            # Compute tracker sets present only on one side of the pair.
            side_sets = tracker_sets[row.pair_id]
            d2_only = side_sets["D2"] - side_sets["EQUIVALENT"]
            equivalent_only = side_sets["EQUIVALENT"] - side_sets["D2"]

            if d2_only:
                n_pairs_with_d2_only += 1
            if equivalent_only:
                n_pairs_with_equivalent_only += 1

            pair_rows.append(
                {
                    "country": row.country,
                    "pair_id": row.pair_id,
                    "d2_etld": row.d2_etld,
                    "equivalent_etld": row.equivalent_etld,
                    "n_d2_only_trackers": len(d2_only),
                    "n_equivalent_only_trackers": len(equivalent_only),
                    "d2_only_trackers": "|".join(sorted(d2_only)),
                    "equivalent_only_trackers": "|".join(sorted(equivalent_only)),
                }
            )

            for tracker_domain in d2_only:
                d2_only_counter[tracker_domain] = d2_only_counter.get(tracker_domain, 0) + 1
            for tracker_domain in equivalent_only:
                equivalent_only_counter[tracker_domain] = equivalent_only_counter.get(tracker_domain, 0) + 1

        # Summarize how often country pairs show tracker asymmetry at all.
        n_pairs = len(country_pairs)
        summary_rows.append(
            {
                "country": country,
                "n_pairs": n_pairs,
                "n_pairs_with_d2_only_trackers": n_pairs_with_d2_only,
                "pct_pairs_with_d2_only_trackers": round((n_pairs_with_d2_only / n_pairs) * 100, 2) if n_pairs else 0.0,
                "n_pairs_with_equivalent_only_trackers": n_pairs_with_equivalent_only,
                "pct_pairs_with_equivalent_only_trackers": round((n_pairs_with_equivalent_only / n_pairs) * 100, 2) if n_pairs else 0.0,
            }
        )

        # Keep the top 10 most common one-sided trackers for each country and side.
        for side, counter in [("D2", d2_only_counter), ("EQUIVALENT", equivalent_only_counter)]:
            for tracker_domain, n_pairs_present_only in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:10]:
                top_rows.append(
                    {
                        "country": country,
                        "side": side,
                        "tracker_domain": tracker_domain,
                        "n_pairs_present_only": n_pairs_present_only,
                        "pct_pairs_present_only": round((n_pairs_present_only / n_pairs) * 100, 2) if n_pairs else 0.0,
                    }
                )

    pd.DataFrame(pair_rows).to_csv(f"{OUTPUT_DIR}/tracker_level_pair_differences.csv", index=False)
    pd.DataFrame(summary_rows).sort_values("country").to_csv(
        f"{OUTPUT_DIR}/tracker_level_difference_summary.csv",
        index=False,
    )
    pd.DataFrame(top_rows).sort_values(["country", "side", "n_pairs_present_only", "tracker_domain"], ascending=[True, True, False, True]).to_csv(
        f"{OUTPUT_DIR}/tracker_level_top10.csv",
        index=False,
    )


def main():
    # Load mapping, sessions, and requests.
    pair_mapping = load_pair_mapping()
    sessions, all_requests, tracker_requests = load_tables()
    print(
        f"[load] mappings={len(pair_mapping):,}, sessions={len(sessions):,}, "
        f"all_requests={len(all_requests):,}, tracker_requests={len(tracker_requests):,}"
    )

    # Apply the min-10-requests filter before constructing the comparable D2 universe.
    filtered_sessions, filtered_all_requests, filtered_tracker_requests = keep_sessions_with_min_requests(
        sessions,
        all_requests,
        tracker_requests,
    )

    # Keep only D2/equivalent pairs that exist in the filtered database snapshot.
    pair_universe = build_pair_universe(filtered_sessions, pair_mapping)


    # Attach the surviving sessions and requests to each valid pair.
    paired_sessions = build_paired_sessions(filtered_sessions, pair_universe)
    paired_session_ids = set(paired_sessions["session_id"].tolist())
    paired_all_requests = filtered_all_requests[filtered_all_requests["session_id"].isin(paired_session_ids)].copy()
    paired_tracker_requests = filtered_tracker_requests[
        filtered_tracker_requests["session_id"].isin(paired_session_ids)
    ].copy()

    print(
        f"[universe] pairs={len(pair_universe):,}, paired_sessions={len(paired_sessions):,}, "
        f"countries={pair_universe['country'].nunique():,}"
    )

    # Build tracker presence at the pair/side level for D2 vs equivalent comparisons.
    pair_tracking = build_pair_tracking(paired_sessions, paired_tracker_requests)
    tracker_sets = build_pair_tracker_sets(pair_universe, pair_tracking)

    # Export site-level and tracker-level summaries.
    save_pair_universe(pair_universe)
    export_overall_numbers(pair_universe)
    pair_level = export_tracking_prevalence(pair_universe, tracker_sets)
    export_site_tracking_difference_summary(pair_level)
    export_tracker_level_differences(pair_universe, tracker_sets)

    # Reuse the same inclusion-chain debugging logic used for D1, but on D2/equivalent pairs.
    export_d2_session_level_deviation_analysis(
        pair_universe,
        paired_sessions,
        paired_all_requests,
        paired_tracker_requests,
        tracker_sets,
        OUTPUT_DIR,
    )


if __name__ == "__main__":
    main()
