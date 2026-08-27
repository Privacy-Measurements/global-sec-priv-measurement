import itertools
import json
import os
import sys
from collections import defaultdict
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from analysis.fingerprinting.general import build_site_fingerprinting_table
from analysis.utils.sessions import filter_global_to_common_etlds
from analysis.utils.urls import extract_domain_from_url

OUTPUT_DIR = os.path.join(CURRENT_DIR, "reports_D1")
COUNTRIES = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
COUNTRY_SET = set(COUNTRIES)
MIN_REQUESTS_PER_SESSION = 10
TOP_K = 10

os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_url(url):
    if not url:
        return ""
    parsed = urlparse(str(url))
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _coerce_string(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        items = []
        for item in value:
            item_str = _coerce_string(item)
            if item_str:
                items.append(item_str)
        return items[0] if len(items) == 1 else None
    return str(value).strip()


def _unique_sorted_strings(values):
    items = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            for nested in value:
                nested_value = _coerce_string(nested)
                if nested_value:
                    items.append(nested_value)
        else:
            item = _coerce_string(value)
            if item:
                items.append(item)
    return sorted(set(items))


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
        WHERE category = 'global'
          AND location NOT LIKE '%%VPN'
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        """,
        engine,
    )
    sessions["fingerprinting"] = sessions["fingerprinting"].apply(
        lambda value: __import__("json").loads(value) if isinstance(value, str) and value.strip() else {}
    )
    sessions["normalized_url"] = sessions["url"].apply(normalize_url)

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
            WHERE category = 'global'
              AND location NOT LIKE '%%VPN'
              AND (is_cloudflare IS NULL OR is_cloudflare = 0)
              AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        )
        """,
        engine,
    )
    return sessions, requests


def keep_sessions_with_min_requests(sessions, all_requests):
    valid_ids = set(
        all_requests.groupby("session_id").size()[
            all_requests.groupby("session_id").size() >= MIN_REQUESTS_PER_SESSION
        ].index
    )
    return sessions[sessions["session_id"].isin(valid_ids)].copy()


def keep_etlds_in_all_countries(sessions):
    etld_country_sets = sessions.groupby("etld")["location"].agg(set)
    keep_etlds = {etld for etld, locs in etld_country_sets.items() if locs == COUNTRY_SET}
    return sessions[sessions["etld"].isin(keep_etlds)].copy()


def keep_etlds_with_non_empty_url_intersection(sessions):
    pairs = set()
    for etld, etld_df in sessions.groupby("etld"):
        country_sets = []
        for country in COUNTRIES:
            country_urls = set(etld_df[etld_df["location"] == country]["normalized_url"].tolist())
            country_sets.append(country_urls)
        common_urls = set.intersection(*country_sets) if country_sets else set()
        if not common_urls:
            continue
        for url_value in common_urls:
            pairs.add((etld, url_value))

    keep_mask = sessions.apply(lambda row: (row["etld"], row["normalized_url"]) in pairs, axis=1)
    return sessions[keep_mask].copy()


def build_strict_d1_universe(sessions, requests):
    s = keep_sessions_with_min_requests(sessions, requests)
    s = keep_etlds_in_all_countries(s)
    s = keep_etlds_with_non_empty_url_intersection(s)
    return s


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
                fingerprinting_url = None
                for key in ("caller_url", "script_src", "script_url", "domain", "url"):
                    value = entry.get(key)
                    if value is None:
                        continue
                    candidate = _coerce_string(value)
                    if not candidate:
                        continue
                    if candidate.startswith("//"):
                        candidate = "https:" + candidate
                    fingerprinting_url = candidate
                    break
                if not fingerprinting_url:
                    continue
                domain = extract_domain_from_url(fingerprinting_url) or fingerprinting_url
                rows.append(
                    {
                        "session_id": row["session_id"],
                        "etld": row["etld"],
                        "category": row["category"],
                        "location": row["location"],
                        "fp_type": str(fp_type).strip(),
                        "fingerprinter": str(domain).strip(),
                        "fingerprinting_url": str(fingerprinting_url).strip(),
                    }
                )
    return pd.DataFrame(rows)


def get_country_group_combinations(countries):
    combos = []
    for size in range(1, len(countries)):
        for group_a in itertools.combinations(countries, size):
            group_b = tuple(country for country in countries if country not in group_a)
            if group_b:
                combos.append((group_a, group_b))
    return combos


def _group_metric_values(site_fingerprinting):
    metric_values = {
        "overall": [None],
        "fp_type": sorted(site_fingerprinting["fp_type"].dropna().unique().tolist()),
        "fingerprinting_domain": sorted(site_fingerprinting["fingerprinter"].dropna().unique().tolist()),
        "fingerprinting_url": sorted(site_fingerprinting["fingerprinting_url"].dropna().unique().tolist()),
        "fp_type__fingerprinting_domain": sorted(
            set((row["fp_type"], row["fingerprinter"]) for _, row in site_fingerprinting.iterrows())
        ),
        "fp_type__fingerprinting_url": sorted(
            set((row["fp_type"], row["fingerprinting_url"]) for _, row in site_fingerprinting.iterrows())
        ),
    }
    return metric_values


def _group_only_presence(site_fingerprinting, group_a, group_b, metric_name, metric_value=None):
    """Return ETLDs that are only in group A or only in group B under the strict D1 rule.

    A site is in group A only if:
    - it is present in every country of group A
    - it is absent from every country in group B

    Similarly for group B only.
    """
    group_a_set = set(group_a)
    group_b_set = set(group_b)
    all_group_locations = group_a_set | group_b_set
    filtered = site_fingerprinting[site_fingerprinting["location"].isin(all_group_locations)].copy()
    if filtered.empty:
        return set(), set()

    if metric_name == "overall":
        matched = filtered
    elif metric_name == "fp_type":
        matched = filtered[filtered["fp_type"] == metric_value]
    elif metric_name == "fingerprinting_domain":
        matched = filtered[filtered["fingerprinter"] == metric_value]
    elif metric_name == "fingerprinting_url":
        matched = filtered[filtered["fingerprinting_url"] == metric_value]
    elif metric_name == "fp_type__fingerprinting_domain":
        fp_type, fingerprinter = metric_value
        matched = filtered[(filtered["fp_type"] == fp_type) & (filtered["fingerprinter"] == fingerprinter)]
    elif metric_name == "fp_type__fingerprinting_url":
        fp_type, fingerprinting_url = metric_value
        matched = filtered[(filtered["fp_type"] == fp_type) & (filtered["fingerprinting_url"] == fingerprinting_url)]
    else:
        raise ValueError(f"Unknown metric_name: {metric_name}")

    if matched.empty:
        return set(), set()

    by_etld = matched.groupby("etld")["location"].agg(set)
    group_a_only = {
        etld
        for etld, country_set in by_etld.items()
        if group_a_set.issubset(country_set) and group_b_set.isdisjoint(country_set)
    }
    group_b_only = {
        etld
        for etld, country_set in by_etld.items()
        if group_b_set.issubset(country_set) and group_a_set.isdisjoint(country_set)
    }
    return group_a_only, group_b_only


def _strict_group_presence(site_fingerprinting, group_countries, metric_name, metric_value=None):
    _, group_b_only = _group_only_presence(site_fingerprinting, group_countries, tuple(), metric_name, metric_value)
    return set()


def _build_group_site_presence(site_fingerprinting, group_countries, metric_name, metric_value=None):
    group_a_only, group_b_only = _group_only_presence(site_fingerprinting, group_countries, tuple(), metric_name, metric_value)
    return group_a_only | group_b_only


def write_group_site_level_data(site_fingerprinting, group_a, group_b, folder):
    group_a_set = set(group_a)
    group_b_set = set(group_b)
    all_group_sites = set(site_fingerprinting[site_fingerprinting["location"].isin(group_a_set | group_b_set)]["etld"].dropna().unique().tolist())

    group_a_presence, group_b_presence = _group_only_presence(site_fingerprinting, group_a, group_b, "overall")

    filtered = site_fingerprinting[site_fingerprinting["location"].isin(group_a_set | group_b_set)].copy()
    if filtered.empty:
        empty_df = pd.DataFrame(columns=[
            "group_a", "group_b", "etld", "location", "fp_type", "fingerprinter",
            "fingerprinting_url", "site_status", "in_group_a", "in_group_b"
        ])
        empty_df.to_csv(os.path.join(folder, "site_level_details.csv"), index=False)
        return empty_df

    def site_status(etld_value):
        in_a = etld_value in group_a_presence
        in_b = etld_value in group_b_presence
        if in_a and not in_b:
            return "group_a_only"
        if in_b and not in_a:
            return "group_b_only"
        if in_a and in_b:
            return "both"
        return "not_in_group"

    filtered["group_a"] = "|".join(group_a)
    filtered["group_b"] = "|".join(group_b)
    filtered["site_status"] = filtered["etld"].map(site_status)
    filtered["in_group_a"] = filtered["etld"].isin(group_a_presence)
    filtered["in_group_b"] = filtered["etld"].isin(group_b_presence)

    columns = [
        "group_a", "group_b", "etld", "location", "fp_type", "fingerprinter",
        "fingerprinting_url", "site_status", "in_group_a", "in_group_b"
    ]
    site_level_df = filtered[columns].copy()
    site_level_df.to_csv(os.path.join(folder, "site_level_details.csv"), index=False)
    return site_level_df


def compare_country_groups(site_fingerprinting, countries, output_dir):
    group_root = os.path.join(output_dir, "groups")
    os.makedirs(group_root, exist_ok=True)

    metric_values = _group_metric_values(site_fingerprinting)
    rows = []
    combination_count = len(get_country_group_combinations(countries))
    print(f"[D1] country-group comparison: {combination_count} group combinations to process")

    for i, (group_a, group_b) in enumerate(get_country_group_combinations(countries), start=1):
        group_label = f"{'|'.join(group_a)}__vs__{'|'.join(group_b)}"
        folder = os.path.join(group_root, group_label)
        os.makedirs(folder, exist_ok=True)
        print(f"[D1] processing group {i}/{combination_count}: {group_label}")

        group_a_set = set(group_a)
        group_b_set = set(group_b)
        all_group_sites = set(site_fingerprinting[site_fingerprinting["location"].isin(group_a_set | group_b_set)]["etld"].dropna().unique().tolist())
        total_group_sites = len(all_group_sites)

        write_group_site_level_data(site_fingerprinting, group_a, group_b, folder)

        for metric_name, values in metric_values.items():
            metric_rows = []
            print(f"  [D1]   metric={metric_name} ({len(values)} values)")
            for metric_value in values:
                group_a_present, group_b_present = _group_only_presence(site_fingerprinting, group_a, group_b, metric_name, metric_value)

                union_sites = group_a_present | group_b_present
                n_a_only = len(group_a_present)
                n_b_only = len(group_b_present)
                n_both = len(set(group_a_present) & set(group_b_present))
                union_total = len(union_sites)
                n_neither = len(all_group_sites - union_sites)

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
                        "n_total_sites_in_group": total_group_sites,
                        "n_total_sites_in_union": union_total,
                        "pct_group_a_only": round((n_a_only / total_group_sites * 100) if total_group_sites else 0.0, 2),
                        "pct_group_b_only": round((n_b_only / total_group_sites * 100) if total_group_sites else 0.0, 2),
                        "pct_both": round((n_both / total_group_sites * 100) if total_group_sites else 0.0, 2),
                        "pct_neither": round((n_neither / total_group_sites * 100) if total_group_sites else 0.0, 2),
                    }
                )
            metric_df = pd.DataFrame(metric_rows)
            metric_df.to_csv(os.path.join(folder, f"{metric_name}.csv"), index=False)
            rows.extend(metric_rows)

    comparison_df = pd.DataFrame(rows)
    comparison_df.to_csv(os.path.join(group_root, "all_group_comparisons.csv"), index=False)
    return comparison_df


def compute_top_difference_sources(site_fingerprinting, countries, output_dir):
    comparison_rows = []
    for group_a, group_b in get_country_group_combinations(countries):
        group_a_set = set(group_a)
        group_b_set = set(group_b)
        group_a_df = site_fingerprinting[site_fingerprinting["location"].isin(group_a_set)].copy()
        group_b_df = site_fingerprinting[site_fingerprinting["location"].isin(group_b_set)].copy()

        metric_specs = [
            ("overall", None),
            ("fp_type", "fp_type"),
            ("fingerprinting_domain", "fingerprinter"),
            ("fingerprinting_url", "fingerprinting_url"),
        ]

        for metric_name, metric_col in metric_specs:
            if metric_name == "overall":
                metric_values = [None]
            elif metric_col == "fp_type":
                metric_values = sorted(site_fingerprinting["fp_type"].dropna().unique().tolist())
            elif metric_col == "fingerprinter":
                metric_values = sorted(site_fingerprinting["fingerprinter"].dropna().unique().tolist())
            else:
                metric_values = sorted(site_fingerprinting["fingerprinting_url"].dropna().unique().tolist())

            for metric_value in metric_values:
                if metric_name == "overall":
                    group_a_present = set(group_a_df["etld"].dropna().unique().tolist())
                    group_b_present = set(group_b_df["etld"].dropna().unique().tolist())
                else:
                    group_a_present = set(group_a_df[group_a_df[metric_col] == metric_value]["etld"].dropna().unique().tolist())
                    group_b_present = set(group_b_df[group_b_df[metric_col] == metric_value]["etld"].dropna().unique().tolist())

                comparison_rows.append(
                    {
                        "group_a": "|".join(group_a),
                        "group_b": "|".join(group_b),
                        "metric_name": metric_name,
                        "metric_value": str(metric_value),
                        "n_group_a_only": len(group_a_present - group_b_present),
                        "n_group_b_only": len(group_b_present - group_a_present),
                    }
                )

    diff_summary = pd.DataFrame(comparison_rows)
    diff_summary.to_csv(os.path.join(output_dir, "top_difference_sources.csv"), index=False)

    for metric_name, output_name in [
        ("fingerprinting_url", "top_fingerprinting_urls_overall.csv"),
        ("fingerprinting_domain", "top_fingerprinting_domains_overall.csv"),
    ]:
        subset = diff_summary[diff_summary["metric_name"] == metric_name].copy()
        counts = defaultdict(int)
        for _, row in subset.iterrows():
            counts[row["metric_value"]] += int(row["n_group_a_only"]) + int(row["n_group_b_only"])

        summary_df = pd.DataFrame([
            {"metric_name": metric_name, "metric_value": key, "n_diff_sites": value}
            for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ])
        if not summary_df.empty:
            total = summary_df["n_diff_sites"].sum()
            summary_df["pct_of_diff_sites"] = (summary_df["n_diff_sites"] / total * 100).round(2) if total else 0.0
        summary_df.to_csv(os.path.join(output_dir, output_name), index=False)

    return diff_summary


def write_group_top_by_type(site_fingerprinting, countries, output_dir):
    by_type = []
    for fp_type in sorted(site_fingerprinting["fp_type"].dropna().unique().tolist()):
        filtered = site_fingerprinting[site_fingerprinting["fp_type"] == fp_type].copy()
        for group_a, group_b in get_country_group_combinations(countries):
            group_a_set = set(group_a)
            group_b_set = set(group_b)
            group_a_present = set(filtered[filtered["location"].isin(group_a_set)]["etld"].dropna().unique().tolist())
            group_b_present = set(filtered[filtered["location"].isin(group_b_set)]["etld"].dropna().unique().tolist())
            by_type.append(
                {
                    "group_a": "|".join(group_a),
                    "group_b": "|".join(group_b),
                    "fp_type": fp_type,
                    "n_group_a_only": len(group_a_present - group_b_present),
                    "n_group_b_only": len(group_b_present - group_a_present),
                    "n_total_sites": len(group_a_present | group_b_present),
                }
            )
    pd.DataFrame(by_type).to_csv(os.path.join(output_dir, "by_type_group_differences.csv"), index=False)

    url_rows = []
    for fp_type in sorted(site_fingerprinting["fp_type"].dropna().unique().tolist()):
        filtered = site_fingerprinting[site_fingerprinting["fp_type"] == fp_type].copy()
        counts = defaultdict(int)
        for group_a, group_b in get_country_group_combinations(countries):
            group_a_set = set(group_a)
            group_b_set = set(group_b)
            a_by_site = filtered[filtered["location"].isin(group_a_set)].groupby("etld")["fingerprinting_url"].agg(set)
            b_by_site = filtered[filtered["location"].isin(group_b_set)].groupby("etld")["fingerprinting_url"].agg(set)
            for etld in set(a_by_site.index) | set(b_by_site.index):
                a_urls = a_by_site.get(etld, set())
                b_urls = b_by_site.get(etld, set())
                for url in a_urls - b_urls:
                    counts[url] += 1
                for url in b_urls - a_urls:
                    counts[url] += 1
        if not counts:
            continue
        frame = pd.DataFrame([
            {"fp_type": fp_type, "fingerprinting_url": url, "n_diff_sites": count}
            for url, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ])
        total = frame["n_diff_sites"].sum()
        if total:
            frame["pct_of_diff_sites"] = (frame["n_diff_sites"] / total * 100).round(2)
        url_rows.append(frame)
    if url_rows:
        pd.concat(url_rows, ignore_index=True).to_csv(os.path.join(output_dir, "top_fingerprinting_urls_by_type.csv"), index=False)

    domain_rows = []
    for fp_type in sorted(site_fingerprinting["fp_type"].dropna().unique().tolist()):
        filtered = site_fingerprinting[site_fingerprinting["fp_type"] == fp_type].copy()
        counts = defaultdict(int)
        for group_a, group_b in get_country_group_combinations(countries):
            group_a_set = set(group_a)
            group_b_set = set(group_b)
            a_by_site = filtered[filtered["location"].isin(group_a_set)].groupby("etld")["fingerprinter"].agg(set)
            b_by_site = filtered[filtered["location"].isin(group_b_set)].groupby("etld")["fingerprinter"].agg(set)
            for etld in set(a_by_site.index) | set(b_by_site.index):
                a_domains = a_by_site.get(etld, set())
                b_domains = b_by_site.get(etld, set())
                for domain in a_domains - b_domains:
                    counts[domain] += 1
                for domain in b_domains - a_domains:
                    counts[domain] += 1
        if not counts:
            continue
        frame = pd.DataFrame([
            {"fp_type": fp_type, "fingerprinting_domain": domain, "n_diff_sites": count}
            for domain, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ])
        total = frame["n_diff_sites"].sum()
        if total:
            frame["pct_of_diff_sites"] = (frame["n_diff_sites"] / total * 100).round(2)
        domain_rows.append(frame)
    if domain_rows:
        pd.concat(domain_rows, ignore_index=True).to_csv(os.path.join(output_dir, "top_fingerprinting_domains_by_type.csv"), index=False)


def compute_prevalence(sessions, site_fingerprinting):
    all_etlds = sessions[["etld", "location"]].drop_duplicates()
    fp_sites = site_fingerprinting[["etld", "location"]].drop_duplicates().assign(has_fingerprinting=1)
    merged = all_etlds.merge(fp_sites, on=["etld", "location"], how="left").fillna({"has_fingerprinting": 0})
    return (
        merged.groupby("location")["has_fingerprinting"]
        .mean()
        .multiply(100)
        .round(2)
    )


def compute_type_prevalence(sessions, site_fingerprinting):
    all_sites = sessions[["etld", "location"]].drop_duplicates()
    fp_types = site_fingerprinting[["etld", "location", "fp_type"]].drop_duplicates()
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
        on=["etld", "location", "fp_type"],
        how="left",
    ).fillna({"has_fingerprint_type": 0})

    type_prevalence = (
        merged.groupby(["fp_type", "location"])["has_fingerprint_type"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
        .reset_index()
    )
    return type_prevalence


def compute_top_fingerprinters(sessions, site_fingerprinting):
    denominators = sessions[["location", "etld"]].drop_duplicates().groupby("location").size().to_dict()
    presence = defaultdict(set)
    for _, row in site_fingerprinting.iterrows():
        presence[(row["location"], row["fingerprinter"])].add(row["etld"])

    rows = []
    for (loc, fingerprinter), etlds in presence.items():
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue
        rows.append({
            "location": loc,
            "fingerprinter": fingerprinter,
            "percentage": round(len(etlds) / denom * 100, 2),
        })

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return pd.DataFrame(columns=["Rank"] + COUNTRIES)

    ranked = ranked.sort_values(["location", "percentage", "fingerprinter"], ascending=[True, False, True])
    ranked["rank"] = ranked.groupby("location").cumcount() + 1
    top_ranked = ranked[ranked["rank"] <= TOP_K].copy()
    top_ranked["entry"] = top_ranked.apply(lambda r: f"{r['fingerprinter']} ({r['percentage']:.2f}%)", axis=1)

    return (
        top_ranked.pivot(index="rank", columns="location", values="entry")
        .reindex(index=range(1, TOP_K + 1), columns=COUNTRIES)
        .reset_index()
        .rename(columns={"rank": "Rank"})
    )


def compute_country_specific_fingerprinters(sessions, site_fingerprinting):
    denominators = sessions[["location", "etld"]].drop_duplicates().groupby("location").size().to_dict()
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
        occupied = sum(len(presence[(loc, fingerprinter)]) for fingerprinter in local_only)
        denom = denominators.get(loc, 0)
        rows.append({
            "country": loc,
            "num_country_specific_fingerprinters": len(local_only),
            "coverage_pct": round(occupied / denom * 100 if denom > 0 else 0, 2),
        })
    return pd.DataFrame(rows)


def compute_country_specific_top_fingerprinters(sessions, site_fingerprinting):
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
        rows.append({
            "location": loc,
            "fingerprinter": fingerprinter,
            "percentage": round(len(etlds) / denom * 100, 2),
        })

    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return pd.DataFrame(columns=COUNTRIES)

    ranked = ranked.sort_values(["location", "percentage", "fingerprinter"], ascending=[True, False, True])
    result = {}
    for loc in COUNTRIES:
        country_rows = ranked[ranked["location"] == loc].head(TOP_K)
        result[loc] = [f"{fp} ({pct:.2f}%)" for fp, pct in zip(country_rows["fingerprinter"], country_rows["percentage"])]
    return pd.DataFrame({country: pd.Series(vals) for country, vals in result.items()}).reindex(columns=COUNTRIES)


def compute_webrtc_country_patterns(sessions, site_fingerprinting):
    webrtc_rows = site_fingerprinting[site_fingerprinting["fp_type"] == "webrtc"].copy()
    if webrtc_rows.empty:
        return pd.DataFrame(columns=["etld", "present_countries", "absent_countries", "n_present", "pattern"])

    presence = (
        webrtc_rows[["etld", "location"]]
        .drop_duplicates()
        .groupby("etld")["location"]
        .agg(lambda values: sorted(set(values)))
        .reset_index(name="present_countries")
    )
    presence["n_present"] = presence["present_countries"].apply(len)
    presence["absent_countries"] = presence["present_countries"].apply(
        lambda values: sorted(set(COUNTRIES) - set(values))
    )
    presence["pattern"] = presence["present_countries"].apply(lambda values: ",".join(values))

    return presence[["etld", "present_countries", "absent_countries", "n_present", "pattern"]].sort_values(
        ["n_present", "etld"], ascending=[False, True]
    )


def compute_webrtc_fingerprinter_patterns(sessions, site_fingerprinting):
    webrtc_rows = site_fingerprinting[site_fingerprinting["fp_type"] == "webrtc"].copy()
    if webrtc_rows.empty:
        return pd.DataFrame(columns=[
            "etld",
            "present_countries",
            "absent_countries",
            "n_present",
            "pattern",
            "domains_by_country",
            "shared_domains",
            "n_shared_domains",
            "all_domains",
            "n_all_domains",
            "same_domain_across_present_countries",
        ])

    country_domains = (
        webrtc_rows[["etld", "location", "fingerprinter"]]
        .drop_duplicates()
        .groupby(["etld", "location"], as_index=False)["fingerprinter"]
        .agg(_unique_sorted_strings)
    )

    records = []
    for etld, etld_df in country_domains.groupby("etld"):
        present = sorted(etld_df["location"].unique().tolist())
        absent = sorted(set(COUNTRIES) - set(present))
        domains_by_country = (
            etld_df.groupby("location")["fingerprinter"]
            .agg(_unique_sorted_strings)
            .to_dict()
        )
        all_domains = sorted({domain for domains in domains_by_country.values() for domain in domains})

        if not present:
            continue

        if len(present) > 1:
            shared_domains = sorted(set.intersection(*(set(domains_by_country[loc]) for loc in present)))
        else:
            shared_domains = sorted(set(domains_by_country[present[0]]))

        records.append({
            "etld": etld,
            "present_countries": present,
            "absent_countries": absent,
            "n_present": len(present),
            "pattern": ",".join(present),
            "domains_by_country": str(domains_by_country),
            "shared_domains": shared_domains,
            "n_shared_domains": len(shared_domains),
            "all_domains": all_domains,
            "n_all_domains": len(all_domains),
            "same_domain_across_present_countries": len(shared_domains) > 0,
        })

    df = pd.DataFrame(records)
    return df.sort_values(["n_present", "etld"], ascending=[False, True]).reset_index(drop=True)


def compute_webrtc_distribution_summary(fingerprinter_patterns):
    if fingerprinter_patterns.empty:
        return pd.DataFrame(columns=["category", "count", "share_pct"])

    summary = fingerprinter_patterns["same_domain_across_present_countries"].value_counts().rename_axis("category").reset_index(name="count")
    summary["category"] = summary["category"].map({True: "same_domain_across_present_countries", False: "different_domains_across_present_countries"})
    summary["share_pct"] = (summary["count"] / summary["count"].sum() * 100).round(2)
    return summary[["category", "count", "share_pct"]].sort_values("count", ascending=False)


def write_examples(sessions, site_fingerprinting):
    by_country = defaultdict(list)
    for _, row in site_fingerprinting.iterrows():
        by_country[row["location"]].append(row)

    for country in COUNTRIES:
        rows = []
        for etld in sorted({row["etld"] for row in by_country.get(country, [])}):
            values = [row["fingerprinter"] for row in by_country.get(country, []) if row["etld"] == etld]
            unique = sorted(set(values))
            rows.append({
                "country": country,
                "etld": etld,
                "fingerprinters": "; ".join(unique),
                "n_fingerprinters": len(unique),
            })
        pd.DataFrame(rows).sort_values(["n_fingerprinters", "etld"], ascending=[False, True]).head(50).to_csv(
            os.path.join(OUTPUT_DIR, f"D1_examples_{country}.csv"),
            index=False,
        )


def write_d1_site_fingerprinting_summary(sessions, site_fingerprinting):
    """Write one row per (location, site) with OR-aggregated fingerprinting presence."""
    site_keys = (
        sessions[["location", "etld"]]
        .drop_duplicates()
        .rename(columns={"etld": "site"})
        .sort_values(["location", "site"])
        .reset_index(drop=True)
    )

    fp_flags = (
        site_fingerprinting[["location", "etld"]]
        .drop_duplicates()
        .assign(is_fingerprinting=1)
        .rename(columns={"etld": "site"})
    )

    summary = site_keys.merge(fp_flags, on=["location", "site"], how="left")
    summary["is_fingerprinting"] = summary["is_fingerprinting"].fillna(0).astype(int)
    summary.to_csv(os.path.join(OUTPUT_DIR, "D1_site_fingerprinting_summary.csv"), index=False)
    return summary


def main():
    print("[D1] Loading sessions and requests...")
    sessions, requests = load_tables()
    print(f"[D1] Loaded {len(sessions)} sessions and {len(requests)} requests")

    print("[D1] Building strict D1 universe...")
    sessions = build_strict_d1_universe(sessions, requests)
    print(f"[D1] Strict D1 universe contains {len(sessions)} sessions")

    print("[D1] Extracting fingerprinting rows from sessions...")
    site_fingerprinting = build_session_fingerprinting_table(sessions)
    site_fingerprinting = site_fingerprinting.drop_duplicates(subset=["etld", "location", "fp_type", "fingerprinter", "fingerprinting_url"])
    print(f"[D1] Extracted {len(site_fingerprinting)} fingerprinting rows across {site_fingerprinting['etld'].nunique()} sites")

    print("[D1] Computing prevalence and top-level summaries...")
    prevalence = compute_prevalence(sessions, site_fingerprinting)
    type_prevalence = compute_type_prevalence(sessions, site_fingerprinting)
    top_df = compute_top_fingerprinters(sessions, site_fingerprinting)
    country_specific_summary = compute_country_specific_fingerprinters(sessions, site_fingerprinting)
    country_specific_top = compute_country_specific_top_fingerprinters(sessions, site_fingerprinting)
    webrtc_patterns = compute_webrtc_country_patterns(sessions, site_fingerprinting)
    webrtc_fingerprinter_patterns = compute_webrtc_fingerprinter_patterns(sessions, site_fingerprinting)
    webrtc_distribution = compute_webrtc_distribution_summary(webrtc_fingerprinter_patterns)
    write_d1_site_fingerprinting_summary(sessions, site_fingerprinting)

    print("[D1] Writing main CSV exports...")
    prevalence.to_frame(name="value").to_csv(os.path.join(OUTPUT_DIR, "D1_fingerprinting_prevalence.csv"), index=True)
    type_prevalence.to_csv(os.path.join(OUTPUT_DIR, "D1_fingerprinting_type_prevalence.csv"), index=False)
    top_df.to_csv(os.path.join(OUTPUT_DIR, "D1_top_fingerprinters.csv"), index=False)
    country_specific_summary.to_csv(os.path.join(OUTPUT_DIR, "D1_country_specific_fingerprinters_summary.csv"), index=False)
    country_specific_top.to_csv(os.path.join(OUTPUT_DIR, "D1_top_country_specific_fingerprinters.csv"), index=False)
    webrtc_patterns.to_csv(os.path.join(OUTPUT_DIR, "D1_webrtc_country_patterns.csv"), index=False)
    webrtc_fingerprinter_patterns.to_csv(os.path.join(OUTPUT_DIR, "D1_webrtc_fingerprinter_patterns.csv"), index=False)
    webrtc_distribution.to_csv(os.path.join(OUTPUT_DIR, "D1_webrtc_fingerprinter_distribution.csv"), index=False)

    print("[D1] Running country-group comparisons...")
    compare_country_groups(site_fingerprinting, COUNTRIES, OUTPUT_DIR)
    print("[D1] Country-group comparison CSVs written")

    print("[D1] Computing top difference sources...")
    compute_top_difference_sources(site_fingerprinting, COUNTRIES, OUTPUT_DIR)
    print("[D1] Top difference sources written")

    print("[D1] Writing type-level group summaries...")
    write_group_top_by_type(site_fingerprinting, COUNTRIES, OUTPUT_DIR)
    print("[D1] Type-level group summaries written")

    print("[D1] Analysis complete")


if __name__ == "__main__":
    main()
