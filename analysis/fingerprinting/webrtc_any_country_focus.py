
import json
import os
import sys
import pandas as pd
from collections import Counter, defaultdict
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

CURRENT_DIR = Path(__file__).resolve().parent
ANALYSIS_ROOT = CURRENT_DIR.parent
if str(ANALYSIS_ROOT) not in sys.path:
    sys.path.insert(0, str(ANALYSIS_ROOT))

from utils.urls import extract_domain_from_url

OUTPUT_ROOT = CURRENT_DIR / "webrtc"
REPORT = "report.txt"

POSITIVE_COUNTRIES = ["ALGERIA", "INDIA", "UAE"]
NEGATIVE_COUNTRIES = ["GERMANY", "USA"]


def load_engine():
    load_dotenv()
    return create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )



def write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def summarize_distribution(values_by_site, denom):
    counts = Counter()
    for values in values_by_site.values():
        for value in values:
            counts[value] += 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    lines = []
    if not ordered:
        lines.append(f"No non-empty intersection found across the {denom} candidate site(s).")
        return lines
    for value, count in ordered:
        pct = (count / denom * 100) if denom else 0.0
        lines.append(f"- {value}: {count} site(s), {pct:.2f}% of {denom} site(s)")
    return lines



def generate_report(group_a, group_b, selected_sites, webrtc_fingerprinting_domains, webrtc_fingerprinting_urls, group_dir):
    
    report_lines = [
        f"Group A: {'|'.join(group_a)}",
        f"Group B: {'|'.join(group_b)}",
        f"WebRTC sites in any A country and no B countries: {len(selected_sites)}",
        "",
        "List of sites:",
    ]

    for site in selected_sites:
        report_lines.append(f"- {site}")
    report_lines.append("")

    report_lines.append("Distribution of fingerprinting domains:")
    report_lines.extend(summarize_distribution(webrtc_fingerprinting_domains, len(selected_sites)))
    report_lines.append("")

    report_lines.append("Distribution of fingerprinting URLs:")
    report_lines.extend(summarize_distribution(webrtc_fingerprinting_urls, len(selected_sites)))

    report_path = group_dir / REPORT
    write_text(report_path, "\n".join(report_lines) + "\n")
    return {
        "report_file": str(report_path),
        "n_selected_sites": len(selected_sites),
        "n_domain_intersection_sites": len(webrtc_fingerprinting_domains),
        "n_url_intersection_sites": len(webrtc_fingerprinting_urls),
    }


def extract_webrtc_values(entries):
    return {
        (extract_domain_from_url(item["caller_url"]) or item["caller_url"], item["caller_url"])
        for item in entries
        if item.get("caller_url")
    }

def build_country_site_values(sessions):

    site_access_values = defaultdict(lambda: defaultdict(set))
    site_values = {}

    for _, row in sessions.iterrows():

        etld = row["etld"]
        access_id = row.get("access_id")
        fp_data = row.get("fingerprinting") or {}        
        entries = fp_data.get("webrtc")
        site_access_values[etld][str(access_id)].update(extract_webrtc_values(entries))

    for etld, access_map in site_access_values.items():
        value_sets = list(access_map.values())
        if not value_sets or not all(value_sets):
            continue

        common_values = set.intersection(*value_sets)
        if common_values:
            site_values[etld] = common_values

    return site_values

def get_sites_with_fp_in_country(country):
    engine = load_engine()
    sessions = pd.read_sql(
        """
        SELECT
            access_id,
            etld,
            fingerprinting
        FROM crawl_sessions
        WHERE category = 'global'
          AND location = %(country)s
          AND location NOT LIKE '%%VPN'
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
        """,
        engine,
        params={"country": country},
    )
    sessions["fingerprinting"] = sessions["fingerprinting"].apply(
        lambda value: json.loads(value) if isinstance(value, str) and value.strip() else {}
    )

    return build_country_site_values(sessions)

def get_any_group_a_webrtc_sites(country_site_values, group_a, group_b):
    group_a_sites = set().union(*(set(country_site_values.get(country, {})) for country in group_a))
    group_b_sites = set().union(*(set(country_site_values.get(country, {})) for country in group_b))
    return sorted(site for site in group_a_sites if site not in group_b_sites)

def get_shared_webrtc_values(country_site_values, site, countries):
    relevant_values = [
        country_site_values.get(country, {}).get(site, set())
        for country in countries
        if site in country_site_values.get(country, {})
    ]
    if not relevant_values:
        return set()
    if len(relevant_values) == 1:
        return relevant_values[0]
    return set.intersection(*relevant_values)

def get_webrtc_fingerprinting(country_site_values, selected_sites, countries):
    domains = {}
    urls = {}

    for site in selected_sites:
        shared_values = get_shared_webrtc_values(country_site_values, site, countries)
        if not shared_values:
            continue

        domains[site] = {domain for domain, _ in shared_values}
        urls[site] = {url for _, url in shared_values}

    return domains, urls

def build_group_report():
    group_a = POSITIVE_COUNTRIES
    group_b = NEGATIVE_COUNTRIES
    group_dir = OUTPUT_ROOT
    group_dir.mkdir(parents=True, exist_ok=True)

    country_site_values = {
        country: get_sites_with_fp_in_country(country)
        for country in set(group_a) | set(group_b)
    }


    selected_sites = get_any_group_a_webrtc_sites(country_site_values, group_a, group_b)

    webrtc_fingerprinting_domains, webrtc_fingerprinting_urls = get_webrtc_fingerprinting(
        country_site_values,
        selected_sites,
        group_a,
    )

    return generate_report(group_a, group_b, selected_sites, webrtc_fingerprinting_domains, webrtc_fingerprinting_urls, group_dir)


if __name__ == "__main__":
    result = build_group_report()
    print(f"Generated report: {result['report_file']}")
