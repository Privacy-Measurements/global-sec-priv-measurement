from collections import defaultdict

import pandas as pd


# Find trackers that appear in only one country.
def compute_country_specific_tracking_domains(site_tracking):
    # Build country+tracker -> set of sites where the tracker appears.
    presence = defaultdict(set)
    for _, row in site_tracking.iterrows():
        presence[(row["location"], row["tracker_domain"])].add(row["etld"])

    # Build tracker -> set of countries where it appears.
    tracker_locations = defaultdict(set)
    for (loc, domain) in presence:
        tracker_locations[domain].add(loc)

    # Count total site-level tracker occurrences per country for coverage denominator.
    total_occurrences = (
        site_tracking
        .groupby(["location", "tracker_domain", "etld"])
        .size()
        .reset_index()
        .groupby("location")
        .size()
        .to_dict()
    )

    # For each country, keep only trackers exclusive to that country and compute coverage.
    summary = []
    for loc in site_tracking["location"].unique():
        local_only = {
            domain
            for (presence_loc, domain) in presence
            if presence_loc == loc and len(tracker_locations[domain]) == 1
        }

        occ = sum(len(presence[(loc, domain)]) for domain in local_only)
        total = total_occurrences.get(loc, 0)

        summary.append(
            {
                "country": loc,
                "num_country_specific_trackers": len(local_only),
                "coverage_pct": round(occ / total * 100 if total > 0 else 0, 2),
            }
        )

    return pd.DataFrame(summary)


# Build a compact summary table for country-specific tracker metrics.
def build_country_specific_tracking_summary(site_tracking, countries):
    # Split data into analysis buckets (overall D1/D2/D3 and each catalog).
    bucket_map = {
        "overall (D1/D2/D3)": site_tracking[
            site_tracking["category"].isin(["global", "country_coded", "country_specific"])
        ].copy(),
        "D1": site_tracking[site_tracking["category"] == "global"].copy(),
        "D2": site_tracking[site_tracking["category"] == "country_coded"].copy(),
        "D3": site_tracking[site_tracking["category"] == "country_specific"].copy(),
        "D2_EQUIVALENT": site_tracking[site_tracking["category"] == "D2_EQUIVALENT"].copy(),
    }

    # Precompute per-bucket country-specific metrics keyed by country.
    bucket_summaries = {}
    for bucket, bucket_df in bucket_map.items():
        bucket_summary = compute_country_specific_tracking_domains(bucket_df)
        bucket_summaries[bucket] = (
            bucket_summary.set_index("country")
            if not bucket_summary.empty else pd.DataFrame()
        )

    # Flatten results to a metric x bucket table with one column per country.
    metric_rows = []
    for metric in ["num_country_specific_trackers", "coverage_pct"]:
        for bucket in bucket_map:
            bucket_summary = bucket_summaries[bucket]
            row = {"metric": metric, "bucket": bucket}
            for country in countries:
                value = None
                if not bucket_summary.empty and country in bucket_summary.index:
                    value = bucket_summary.loc[country, metric]
                row[country] = value
            metric_rows.append(row)

    return pd.DataFrame(metric_rows)


# Build top country-specific tracker lists per country.
def compute_country_specific_top_trackers(sessions, site_tracking, countries, top_k):
    # Count total distinct sites per country for prevalence percentages.
    denominators = (
        sessions[["location", "etld"]]
        .drop_duplicates()
        .groupby("location")
        .size()
        .to_dict()
    )

    # Build country+tracker -> sites and tracker -> countries maps.
    presence = defaultdict(set)
    for _, row in site_tracking.iterrows():
        presence[(row["location"], row["tracker_domain"])].add(row["etld"])

    tracker_locations = defaultdict(set)
    for (loc, domain) in presence:
        tracker_locations[domain].add(loc)

    # Keep only trackers exclusive to a single country and compute per-country percentage.
    rows = []
    for (loc, domain), etlds in presence.items():
        if len(tracker_locations[domain]) != 1:
            continue

        denom = denominators.get(loc, 0)
        if denom == 0:
            continue

        rows.append(
            {
                "location": loc,
                "tracker": domain,
                "percentage": round(len(etlds) / denom * 100, 2),
                "etld_count": len(etlds),
            }
        )

    # Early return with stable columns when there is no qualifying tracker.
    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return pd.DataFrame(columns=countries)

    ranked = ranked.sort_values(
        ["location", "percentage", "tracker"],
        ascending=[True, False, True],
    )

    # Select top-k trackers per country and assign display rank.
    top_by_country = []
    for loc, group in ranked.groupby("location", sort=False):
        top = group.head(top_k).copy()
        top["rank"] = range(1, len(top) + 1)
        top_by_country.append(top)

    top_ranked = pd.concat(top_by_country, ignore_index=True)

    # Build a wide table where each country column lists top tracker strings.
    list_columns = {}
    for loc in countries:
        group = top_ranked[top_ranked["location"] == loc].sort_values("rank")
        list_columns[loc] = [
            f"{record.tracker} ({record.percentage:.2f}%)"
            for record in group.itertuples()
        ]

    list_df = pd.DataFrame({k: pd.Series(v) for k, v in list_columns.items()})
    return list_df.reindex(columns=countries)


# Write top country-specific tracker outputs for overall and each bucket.
def write_country_specific_top_tracker_outputs(
    sessions,
    site_tracking,
    output_dir,
    countries,
    top_k,
):
    # Define output buckets for overall and each catalog.
    bucket_map = {
        "": (
            sessions[sessions["category"].isin(["global", "country_coded", "country_specific"])].copy(),
            site_tracking[site_tracking["category"].isin(["global", "country_coded", "country_specific"])].copy(),
        ),
        "_d1": (
            sessions[sessions["category"] == "global"].copy(),
            site_tracking[site_tracking["category"] == "global"].copy(),
        ),
        "_d2": (
            sessions[sessions["category"] == "country_coded"].copy(),
            site_tracking[site_tracking["category"] == "country_coded"].copy(),
        ),
        "_d3": (
            sessions[sessions["category"] == "country_specific"].copy(),
            site_tracking[site_tracking["category"] == "country_specific"].copy(),
        ),
        "_d2_equivalent": (
            sessions[sessions["category"] == "D2_EQUIVALENT"].copy(),
            site_tracking[site_tracking["category"] == "D2_EQUIVALENT"].copy(),
        ),
    }

    # Compute and write one top-tracker list CSV per bucket.
    for suffix, (bucket_sessions, bucket_tracking) in bucket_map.items():
        list_df = compute_country_specific_top_trackers(
            bucket_sessions,
            bucket_tracking,
            countries,
            top_k,
        )
        if suffix == "":
            list_name = "top_country_specific_trackers_overall_d1_d2_d3.csv"
        else:
            list_name = f"top_country_specific_trackers{suffix}.csv"
        list_df.to_csv(
            f"{output_dir}/{list_name}",
            index=False,
        )


def run_country_specific_outputs(sessions, site_tracking, output_dir, countries, top_k):
    # Write the summary table, then write top-tracker tables by bucket.
    build_country_specific_tracking_summary(site_tracking, countries).to_csv(
        f"{output_dir}/country_specific_tracking_summary.csv",
        index=False,
    )
    
    write_country_specific_top_tracker_outputs(
        sessions,
        site_tracking,
        output_dir,
        countries,
        top_k,
    )