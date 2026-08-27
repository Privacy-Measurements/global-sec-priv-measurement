import itertools
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

from analysis.utils.urls import extract_domain_from_url

OUTPUT_DIR = os.path.join(CURRENT_DIR, "reports_D2")
D2_MAPPING_DIR = os.path.join(REPO_ROOT, "site_catalogs_construction", "catalogs", "D2_to_global")
COUNTRIES = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
COUNTRY_CODE_TO_NAME = {
    "DZ": "ALGERIA",
    "DE": "GERMANY",
    "IN": "INDIA",
    "AE": "UAE",
    "US": "USA",
}
MIN_REQUESTS_PER_SESSION = 10
TOP_K = 10

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_pair_mapping():
    frames = []
    for code, country in COUNTRY_CODE_TO_NAME.items():
        path = os.path.join(D2_MAPPING_DIR, f"d2_to_global_{code}.csv")
        mapping = pd.read_csv(path)
        mapping["country"] = country
        mapping["equivalent_category"] = mapping["in_d1"].apply(
            lambda value: "global" if str(value).strip().lower() == "true" else "D2_EQUIVALENT"
        )
        mapping = mapping.rename(columns={"d2_site": "d2_etld", "global_site": "equivalent_etld"})
        frames.append(mapping[["country", "d2_etld", "equivalent_etld", "in_d1", "equivalent_category"]])

    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(["country", "d2_etld", "equivalent_etld", "equivalent_category"]).copy()


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
            fingerprinting
        FROM crawl_sessions
        WHERE category IN ('country_coded', 'global', 'D2_EQUIVALENT')
          AND location NOT LIKE '%%VPN'
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
        """,
        engine,
    )

    requests = pd.read_sql(
        """
        SELECT
            id,
            session_id,
            request_url,
            is_tracker,
            etld_request_url_rel
        FROM requests
        WHERE session_id IN (
            SELECT id
            FROM crawl_sessions
            WHERE category IN ('country_coded', 'global', 'D2_EQUIVALENT')
              AND location NOT LIKE '%%VPN'
              AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
              AND (is_cloudflare IS NULL OR is_cloudflare = 0)
        )
        """,
        engine,
    )

    sessions["fingerprinting"] = sessions["fingerprinting"].apply(
        lambda value: __import__("json").loads(value) if isinstance(value, str) and value.strip() else {}
    )
    return sessions, requests


def keep_sessions_with_min_requests(sessions, requests):
    counts = requests.groupby("session_id").size()
    valid = set(counts[counts >= MIN_REQUESTS_PER_SESSION].index)
    return sessions[sessions["session_id"].isin(valid)].copy()


def build_pair_universe(sessions, mapping):
    session_sites = sessions[["session_id", "location", "etld", "category"]].drop_duplicates()

    d2_sites = (
        session_sites[session_sites["category"] == "country_coded"]
        .groupby(["location", "etld"])["session_id"]
        .nunique()
        .reset_index(name="n_d2_sessions")
        .rename(columns={"location": "country", "etld": "d2_etld"})
    )

    equivalent_sites = (
        session_sites[session_sites["category"].isin(["global", "D2_EQUIVALENT"]) ]
        .groupby(["location", "etld", "category"])["session_id"]
        .nunique()
        .reset_index(name="n_equivalent_sessions")
        .rename(columns={"location": "country", "etld": "equivalent_etld", "category": "equivalent_category"})
    )

    pair_universe = mapping.merge(d2_sites, on=["country", "d2_etld"], how="inner").merge(
        equivalent_sites,
        on=["country", "equivalent_etld", "equivalent_category"],
        how="inner",
    )
    pair_universe = pair_universe.drop_duplicates(["country", "d2_etld", "equivalent_etld", "equivalent_category"]).copy()
    pair_universe["pair_id"] = pair_universe.apply(
        lambda row: f"{row['country']}|{row['d2_etld']}|{row['equivalent_etld']}|{row['equivalent_category']}",
        axis=1,
    )
    return pair_universe


def build_paired_sessions(sessions, pair_universe):
    d2_sessions = sessions[sessions["category"] == "country_coded"].merge(
        pair_universe[["pair_id", "country", "d2_etld", "equivalent_etld", "equivalent_category"]],
        left_on=["location", "etld"],
        right_on=["country", "d2_etld"],
        how="inner",
    )
    d2_sessions["site_side"] = "D2"

    equivalent_sessions = sessions[sessions["category"].isin(["global", "D2_EQUIVALENT"])].merge(
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
    paired = pd.concat([d2_sessions[columns], equivalent_sessions[columns]], ignore_index=True)
    return paired.drop_duplicates(["pair_id", "site_side", "session_id"]).copy()


def build_session_fingerprinting_table(sessions):
    rows = []
    for _, row in sessions.iterrows():
        fp_data = row.get("fingerprinting") or {}
        if not isinstance(fp_data, dict):
            continue
        for fp_type, entries in fp_data.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                candidate_url = None
                for key in ("caller_url", "script_src", "script_url", "domain", "url"):
                    value = entry.get(key)
                    if not value:
                        continue
                    value = str(value).strip()
                    if value.startswith("//"):
                        value = "https:" + value
                    candidate_url = value
                    break
                if not candidate_url:
                    continue
                candidate = extract_domain_from_url(candidate_url) or candidate_url
                rows.append(
                    {
                        "session_id": row["session_id"],
                        "etld": row["etld"],
                        "category": row["category"],
                        "location": row["location"],
                        "fp_type": fp_type,
                        "fingerprinter": candidate,
                        "fingerprinting_url": candidate_url,
                    }
                )
    if not rows:
        return pd.DataFrame(columns=["session_id", "etld", "category", "location", "fp_type", "fingerprinter", "fingerprinting_url"])
    return pd.DataFrame(rows).drop_duplicates(subset=["session_id", "etld", "category", "location", "fp_type", "fingerprinter", "fingerprinting_url"])


def get_country_group_combinations(countries):
    combos = []
    for size in range(1, len(countries)):
        for group_a in itertools.combinations(countries, size):
            group_b = tuple(country for country in countries if country not in group_a)
            if group_b:
                combos.append((group_a, group_b))
    return combos


def _group_metric_values(site_fingerprinting):
    return {
        "overall": [None],
        "fp_type": sorted(site_fingerprinting["fp_type"].dropna().unique().tolist()),
        "fingerprinting_domain": sorted(site_fingerprinting["fingerprinter"].dropna().unique().tolist()),
        "fingerprinting_url": sorted(site_fingerprinting["fingerprinting_url"].dropna().unique().tolist()),
    }


def _build_group_site_presence(site_fingerprinting, group_countries, metric_name, metric_value=None):
    filtered = site_fingerprinting[site_fingerprinting["location"].isin(group_countries)].copy()
    if metric_name == "overall":
        return set(filtered["etld"].dropna().unique().tolist())
    if metric_name == "fp_type":
        filtered = filtered[filtered["fp_type"] == metric_value]
        return set(filtered["etld"].dropna().unique().tolist())
    if metric_name == "fingerprinting_domain":
        filtered = filtered[filtered["fingerprinter"] == metric_value]
        return set(filtered["etld"].dropna().unique().tolist())
    if metric_name == "fingerprinting_url":
        filtered = filtered[filtered["fingerprinting_url"] == metric_value]
        return set(filtered["etld"].dropna().unique().tolist())
    raise ValueError(f"Unknown metric_name: {metric_name}")


def compare_country_groups(site_fingerprinting, countries, output_dir):
    group_root = os.path.join(output_dir, "groups")
    os.makedirs(group_root, exist_ok=True)
    rows = []
    for group_a, group_b in get_country_group_combinations(countries):
        group_label = f"{'|'.join(group_a)}__vs__{'|'.join(group_b)}"
        folder = os.path.join(group_root, group_label)
        os.makedirs(folder, exist_ok=True)
        for metric_name, values in _group_metric_values(site_fingerprinting).items():
            metric_rows = []
            for metric_value in values:
                group_a_present = _build_group_site_presence(site_fingerprinting, group_a, metric_name, metric_value)
                group_b_present = _build_group_site_presence(site_fingerprinting, group_b, metric_name, metric_value)
                union_sites = group_a_present | group_b_present
                total = len(union_sites)
                n_a_only = len(group_a_present - group_b_present)
                n_b_only = len(group_b_present - group_a_present)
                n_both = len(group_a_present & group_b_present)
                n_neither = len(union_sites - (group_a_present | group_b_present))
                metric_rows.append(
                    {
                        "group_a": "|".join(group_a),
                        "group_b": "|".join(group_b),
                        "metric_name": metric_name,
                        "metric_value": str(metric_value),
                        "n_group_a_only": n_a_only,
                        "n_group_b_only": n_b_only,
                        "n_both": n_both,
                        "n_neither": n_neither,
                        "n_total_sites_in_union": total,
                        "pct_group_a_only": round((n_a_only / total * 100) if total else 0.0, 2),
                        "pct_group_b_only": round((n_b_only / total * 100) if total else 0.0, 2),
                        "pct_both": round((n_both / total * 100) if total else 0.0, 2),
                        "pct_neither": round((n_neither / total * 100) if total else 0.0, 2),
                    }
                )
            metric_df = pd.DataFrame(metric_rows)
            metric_df.to_csv(os.path.join(folder, f"{metric_name}.csv"), index=False)
            rows.extend(metric_rows)
    pd.DataFrame(rows).to_csv(os.path.join(group_root, "all_group_comparisons.csv"), index=False)
    return rows


def compute_top_difference_sources(site_fingerprinting, countries, output_dir):
    rows = []
    for group_a, group_b in get_country_group_combinations(countries):
        group_a_set = set(group_a)
        group_b_set = set(group_b)
        comparison_sites = set(site_fingerprinting[(site_fingerprinting["location"].isin(group_a_set | group_b_set))]["etld"].unique())
        for metric_name in ["overall", "fp_type", "fingerprinting_domain", "fingerprinting_url"]:
            values = [None] if metric_name == "overall" else sorted(site_fingerprinting["fp_type"].dropna().unique().tolist() if metric_name == "fp_type" else site_fingerprinting["fingerprinter"].dropna().unique().tolist() if metric_name == "fingerprinting_domain" else site_fingerprinting["fingerprinting_url"].dropna().unique().tolist())
            for metric_value in values:
                group_a_present = set()
                group_b_present = set()
                for etld in comparison_sites:
                    a_rows = site_fingerprinting[(site_fingerprinting["etld"] == etld) & (site_fingerprinting["location"].isin(group_a_set))].copy()
                    b_rows = site_fingerprinting[(site_fingerprinting["etld"] == etld) & (site_fingerprinting["location"].isin(group_b_set))].copy()
                    if metric_name == "overall":
                        a_present = not a_rows.empty
                        b_present = not b_rows.empty
                    elif metric_name == "fp_type":
                        a_present = ((a_rows["fp_type"] == metric_value).any())
                        b_present = ((b_rows["fp_type"] == metric_value).any())
                    elif metric_name == "fingerprinting_domain":
                        a_present = ((a_rows["fingerprinter"] == metric_value).any())
                        b_present = ((b_rows["fingerprinter"] == metric_value).any())
                    else:
                        a_present = ((a_rows["fingerprinting_url"] == metric_value).any())
                        b_present = ((b_rows["fingerprinting_url"] == metric_value).any())
                    if a_present:
                        group_a_present.add(etld)
                    if b_present:
                        group_b_present.add(etld)
                rows.append(
                    {
                        "group_a": "|".join(group_a),
                        "group_b": "|".join(group_b),
                        "metric_name": metric_name,
                        "metric_value": str(metric_value),
                        "n_group_a_only": len(group_a_present - group_b_present),
                        "n_group_b_only": len(group_b_present - group_a_present),
                    }
                )
    diff_summary = pd.DataFrame(rows)
    diff_summary.to_csv(os.path.join(output_dir, "top_difference_sources.csv"), index=False)

    url_df = diff_summary[diff_summary["metric_name"] == "fingerprinting_url"].copy()
    url_counts = defaultdict(int)
    for _, row in url_df.iterrows():
        url_counts[row["metric_value"]] += row["n_group_a_only"] + row["n_group_b_only"]
    if url_counts:
        url_summary = pd.DataFrame([
            {"fingerprinting_url": key, "n_diff_sites": value}
            for key, value in sorted(url_counts.items(), key=lambda item: (-item[1], item[0]))
        ])
        total = url_summary["n_diff_sites"].sum()
        url_summary["pct_of_diff_sites"] = (url_summary["n_diff_sites"] / total * 100).round(2) if total else 0.0
        url_summary.to_csv(os.path.join(output_dir, "top_fingerprinting_urls_overall.csv"), index=False)

    domain_counts = defaultdict(int)
    domain_df = diff_summary[diff_summary["metric_name"] == "fingerprinting_domain"].copy()
    for _, row in domain_df.iterrows():
        domain_counts[row["metric_value"]] += row["n_group_a_only"] + row["n_group_b_only"]
    if domain_counts:
        domain_summary = pd.DataFrame([
            {"fingerprinting_domain": key, "n_diff_sites": value}
            for key, value in sorted(domain_counts.items(), key=lambda item: (-item[1], item[0]))
        ])
        total = domain_summary["n_diff_sites"].sum()
        domain_summary["pct_of_diff_sites"] = (domain_summary["n_diff_sites"] / total * 100).round(2) if total else 0.0
        domain_summary.to_csv(os.path.join(output_dir, "top_fingerprinting_domains_overall.csv"), index=False)

    return diff_summary


def build_group_type_specific_rankings(site_fingerprinting, countries, output_dir):
    by_type_rows = []
    by_url_rows = []
    by_domain_rows = []

    for fp_type in sorted(site_fingerprinting["fp_type"].dropna().unique().tolist()):
        filtered = site_fingerprinting[site_fingerprinting["fp_type"] == fp_type].copy()
        for group_a, group_b in get_country_group_combinations(countries):
            group_a_set = set(group_a)
            group_b_set = set(group_b)
            a_present = set(filtered[filtered["location"].isin(group_a_set)]["etld"].unique())
            b_present = set(filtered[filtered["location"].isin(group_b_set)]["etld"].unique())
            by_type_rows.append(
                {
                    "fp_type": fp_type,
                    "group_a": "|".join(group_a),
                    "group_b": "|".join(group_b),
                    "n_group_a_only": len(a_present - b_present),
                    "n_group_b_only": len(b_present - a_present),
                    "n_total_sites_in_union": len(a_present | b_present),
                }
            )

        url_counts = defaultdict(int)
        for group_a, group_b in get_country_group_combinations(countries):
            group_a_set = set(group_a)
            group_b_set = set(group_b)
            comparison_sites = set(filtered[(filtered["location"].isin(group_a_set | group_b_set))]["etld"].unique())
            for etld in comparison_sites:
                a_urls = set(filtered[(filtered["etld"] == etld) & (filtered["location"].isin(group_a_set))]["fingerprinting_url"].unique())
                b_urls = set(filtered[(filtered["etld"] == etld) & (filtered["location"].isin(group_b_set))]["fingerprinting_url"].unique())
                for url in a_urls - b_urls:
                    url_counts[url] += 1
                for url in b_urls - a_urls:
                    url_counts[url] += 1
        if url_counts:
            url_frame = pd.DataFrame([
                {"fp_type": fp_type, "fingerprinting_url": url, "n_diff_sites": count}
                for url, count in sorted(url_counts.items(), key=lambda item: (-item[1], item[0]))
            ])
            total = url_frame["n_diff_sites"].sum()
            if total:
                url_frame["pct_of_diff_sites"] = (url_frame["n_diff_sites"] / total * 100).round(2)
            by_url_rows.append(url_frame)

        domain_counts = defaultdict(int)
        for group_a, group_b in get_country_group_combinations(countries):
            group_a_set = set(group_a)
            group_b_set = set(group_b)
            comparison_sites = set(filtered[(filtered["location"].isin(group_a_set | group_b_set))]["etld"].unique())
            for etld in comparison_sites:
                a_domains = set(filtered[(filtered["etld"] == etld) & (filtered["location"].isin(group_a_set))]["fingerprinter"].unique())
                b_domains = set(filtered[(filtered["etld"] == etld) & (filtered["location"].isin(group_b_set))]["fingerprinter"].unique())
                for domain in a_domains - b_domains:
                    domain_counts[domain] += 1
                for domain in b_domains - a_domains:
                    domain_counts[domain] += 1
        if domain_counts:
            domain_frame = pd.DataFrame([
                {"fp_type": fp_type, "fingerprinting_domain": domain, "n_diff_sites": count}
                for domain, count in sorted(domain_counts.items(), key=lambda item: (-item[1], item[0]))
            ])
            total = domain_frame["n_diff_sites"].sum()
            if total:
                domain_frame["pct_of_diff_sites"] = (domain_frame["n_diff_sites"] / total * 100).round(2)
            by_domain_rows.append(domain_frame)

    pd.DataFrame(by_type_rows).to_csv(os.path.join(output_dir, "by_type_group_differences.csv"), index=False)
    if by_url_rows:
        pd.concat(by_url_rows, ignore_index=True).to_csv(os.path.join(output_dir, "top_fingerprinting_urls_by_type.csv"), index=False)
    if by_domain_rows:
        pd.concat(by_domain_rows, ignore_index=True).to_csv(os.path.join(output_dir, "top_fingerprinting_domains_by_type.csv"), index=False)


def main():
    sessions, requests = load_tables()
    sessions = keep_sessions_with_min_requests(sessions, requests)

    pair_mapping = load_pair_mapping()
    pair_universe = build_pair_universe(sessions, pair_mapping)
    paired_sessions = build_paired_sessions(sessions, pair_universe)
    site_fingerprinting = build_session_fingerprinting_table(sessions)
    site_fingerprinting = site_fingerprinting.drop_duplicates(subset=["session_id", "etld", "category", "location", "fp_type", "fingerprinter", "fingerprinting_url"])

    pair_fingerprinting = build_pair_fingerprinting(paired_sessions, site_fingerprinting)
    pair_sets = build_pair_fingerprinter_sets(pair_universe, pair_fingerprinting)

    prevalence = summarize_pair_prevalence(pair_universe, pair_sets)
    type_prevalence = compute_type_prevalence(site_fingerprinting)
    top_df = compute_top_fingerprinters(site_fingerprinting)
    country_specific_summary = compute_country_specific_fingerprinters(site_fingerprinting)
    country_specific_top = compute_country_specific_top_fingerprinters(site_fingerprinting)

    prevalence.to_csv(os.path.join(OUTPUT_DIR, "D2_fingerprinting_prevalence.csv"), index=False)
    type_prevalence.to_csv(os.path.join(OUTPUT_DIR, "D2_fingerprinting_type_prevalence.csv"), index=False)
    top_df.to_csv(os.path.join(OUTPUT_DIR, "D2_top_fingerprinters.csv"), index=False)
    country_specific_summary.to_csv(os.path.join(OUTPUT_DIR, "D2_country_specific_fingerprinters_summary.csv"), index=False)
    country_specific_top.to_csv(os.path.join(OUTPUT_DIR, "D2_top_country_specific_fingerprinters.csv"), index=False)
    compare_country_groups(site_fingerprinting, COUNTRIES, OUTPUT_DIR)
    compute_top_difference_sources(site_fingerprinting, COUNTRIES, OUTPUT_DIR)
    build_group_type_specific_rankings(site_fingerprinting, COUNTRIES, OUTPUT_DIR)
    write_examples(site_fingerprinting)


if __name__ == "__main__":
    main()


def build_pair_fingerprinting(paired_sessions, site_fingerprinting):
    pair_meta = paired_sessions[["pair_id", "site_side", "session_id", "country", "d2_etld", "equivalent_etld"]].drop_duplicates()
    pair_fp = pair_meta.merge(site_fingerprinting, on="session_id", how="inner")
    return pair_fp[["pair_id", "country", "d2_etld", "equivalent_etld", "site_side", "etld", "location", "fingerprinter"]].drop_duplicates()


def build_pair_fingerprinter_sets(pair_universe, pair_fingerprinting):
    tracker_sets = {row.pair_id: {"D2": set(), "EQUIVALENT": set()} for row in pair_universe.itertuples(index=False)}
    grouped = pair_fingerprinting.groupby(["pair_id", "site_side"])["fingerprinter"].agg(set)
    for (pair_id, site_side), fingerprinters in grouped.items():
        if pair_id in tracker_sets:
            tracker_sets[pair_id][site_side] = set(fingerprinters)
    return tracker_sets


def summarize_pair_prevalence(pair_universe, pair_fingerprinter_sets):
    rows = []
    for country in COUNTRIES:
        country_pairs = pair_universe[pair_universe["country"] == country].copy()
        d2_with = 0
        equivalent_with = 0
        for _, row in country_pairs.iterrows():
            fp_sets = pair_fingerprinter_sets.get(row["pair_id"], {"D2": set(), "EQUIVALENT": set()})
            d2_with += int(bool(fp_sets["D2"]))
            equivalent_with += int(bool(fp_sets["EQUIVALENT"]))
        rows.append(
            {
                "country": country,
                "n_pairs": len(country_pairs),
                "n_d2_with_fingerprinting": d2_with,
                "n_equivalent_with_fingerprinting": equivalent_with,
                "pct_d2_with_fingerprinting": round((d2_with / len(country_pairs) * 100) if len(country_pairs) else 0, 2),
                "pct_equivalent_with_fingerprinting": round((equivalent_with / len(country_pairs) * 100) if len(country_pairs) else 0, 2),
            }
        )
    return pd.DataFrame(rows)


def compute_type_prevalence(site_fingerprinting):
    all_sites = site_fingerprinting[["location", "etld"]].drop_duplicates()
    fp_types = site_fingerprinting[["location", "etld", "fp_type"]].drop_duplicates()
    fp_type_values = pd.Index(fp_types["fp_type"].dropna().unique())

    if fp_type_values.empty:
        return pd.DataFrame(columns=["fp_type", *COUNTRIES])

    site_type_grid = all_sites.assign(_key=1).merge(
        pd.DataFrame({"fp_type": fp_type_values, "_key": 1}),
        on="_key",
        how="inner",
    ).drop(columns=["_key"])

    merged = site_type_grid.merge(
        fp_types.assign(has_fingerprint_type=1),
        on=["location", "etld", "fp_type"],
        how="left",
    ).fillna({"has_fingerprint_type": 0})

    return (
        merged.groupby(["fp_type", "location"])["has_fingerprint_type"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
        .reset_index()
    )


def compute_country_specific_fingerprinters(site_fingerprinting):
    presence = defaultdict(set)
    for _, row in site_fingerprinting.iterrows():
        presence[(row["location"], row["fingerprinter"])].add(row["etld"])

    tracker_locations = defaultdict(set)
    for (loc, fingerprinter) in presence:
        tracker_locations[fingerprinter].add(loc)

    rows = []
    for loc in COUNTRIES:
        local_only = {
            fingerprinter
            for (presence_loc, fingerprinter) in presence
            if presence_loc == loc and len(tracker_locations[fingerprinter]) == 1
        }
        occ = sum(len(presence[(loc, fingerprinter)]) for fingerprinter in local_only)
        denom = site_fingerprinting[site_fingerprinting["location"] == loc]["etld"].nunique()
        rows.append(
            {
                "country": loc,
                "num_country_specific_fingerprinters": len(local_only),
                "coverage_pct": round((occ / denom * 100) if denom else 0, 2),
            }
        )
    return pd.DataFrame(rows)


def compute_top_fingerprinters(site_fingerprinting):
    presence = defaultdict(set)
    for _, row in site_fingerprinting.iterrows():
        presence[(row["location"], row["fingerprinter"])].add(row["etld"])

    denominators = site_fingerprinting[["location", "etld"]].drop_duplicates().groupby("location").size().to_dict()
    rows = []
    for (loc, fp), etlds in presence.items():
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue
        rows.append({"location": loc, "fingerprinter": fp, "percentage": round(len(etlds) / denom * 100, 2)})

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return pd.DataFrame(columns=["Rank"] + COUNTRIES)

    ranked = ranked.sort_values(["location", "percentage", "fingerprinter"], ascending=[True, False, True])
    ranked["rank"] = ranked.groupby("location").cumcount() + 1
    top_ranked = ranked[ranked["rank"] <= TOP_K].copy()
    top_ranked["entry"] = top_ranked.apply(lambda row: f"{row['fingerprinter']} ({row['percentage']:.2f}%)", axis=1)
    return (
        top_ranked.pivot(index="rank", columns="location", values="entry")
        .reindex(index=range(1, TOP_K + 1), columns=COUNTRIES)
        .reset_index()
        .rename(columns={"rank": "Rank"})
    )


def compute_country_specific_top_fingerprinters(site_fingerprinting):
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
        denom = site_fingerprinting[site_fingerprinting["location"] == loc]["etld"].nunique()
        if denom == 0:
            continue
        rows.append({"location": loc, "fingerprinter": fingerprinter, "percentage": round(len(etlds) / denom * 100, 2)})

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return pd.DataFrame(columns=COUNTRIES)

    ranked = ranked.sort_values(["location", "percentage", "fingerprinter"], ascending=[True, False, True])
    result = {}
    for loc in COUNTRIES:
        rows_for_country = ranked[ranked["location"] == loc].head(TOP_K)
        result[loc] = [f"{fp} ({pct:.2f}%)" for fp, pct in zip(rows_for_country["fingerprinter"], rows_for_country["percentage"])]
    return pd.DataFrame({country: pd.Series(vals) for country, vals in result.items()}).reindex(columns=COUNTRIES)


def write_examples(site_fingerprinting):
    for country in COUNTRIES:
        rows = []
        country_fps = site_fingerprinting[site_fingerprinting["location"] == country].copy()
        for etld in sorted(country_fps["etld"].unique()):
            fp_values = sorted(set(country_fps[country_fps["etld"] == etld]["fingerprinter"].tolist()))
            rows.append({"country": country, "etld": etld, "fingerprinters": "; ".join(fp_values), "n_fingerprinters": len(fp_values)})
        pd.DataFrame(rows).sort_values(["n_fingerprinters", "etld"], ascending=[False, True]).head(50).to_csv(
            os.path.join(OUTPUT_DIR, f"D2_examples_{country}.csv"),
            index=False,
        )


def main():
    sessions, requests = load_tables()
    sessions = keep_sessions_with_min_requests(sessions, requests)

    pair_mapping = load_pair_mapping()
    pair_universe = build_pair_universe(sessions, pair_mapping)
    paired_sessions = build_paired_sessions(sessions, pair_universe)
    site_fingerprinting = build_session_fingerprinting_table(sessions)
    site_fingerprinting = site_fingerprinting.drop_duplicates(subset=["session_id", "etld", "category", "location", "fingerprinter"])

    pair_fingerprinting = build_pair_fingerprinting(paired_sessions, site_fingerprinting)
    pair_sets = build_pair_fingerprinter_sets(pair_universe, pair_fingerprinting)

    prevalence = summarize_pair_prevalence(pair_universe, pair_sets)
    type_prevalence = compute_type_prevalence(site_fingerprinting)
    top_df = compute_top_fingerprinters(site_fingerprinting)
    country_specific_summary = compute_country_specific_fingerprinters(site_fingerprinting)
    country_specific_top = compute_country_specific_top_fingerprinters(site_fingerprinting)

    prevalence.to_csv(os.path.join(OUTPUT_DIR, "D2_fingerprinting_prevalence.csv"), index=False)
    type_prevalence.to_csv(os.path.join(OUTPUT_DIR, "D2_fingerprinting_type_prevalence.csv"), index=False)
    top_df.to_csv(os.path.join(OUTPUT_DIR, "D2_top_fingerprinters.csv"), index=False)
    country_specific_summary.to_csv(os.path.join(OUTPUT_DIR, "D2_country_specific_fingerprinters_summary.csv"), index=False)
    country_specific_top.to_csv(os.path.join(OUTPUT_DIR, "D2_top_country_specific_fingerprinters.csv"), index=False)
    write_examples(site_fingerprinting)


if __name__ == "__main__":
    main()
