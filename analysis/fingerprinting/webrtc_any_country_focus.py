
from collections import Counter
from pathlib import Path

import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = CURRENT_DIR / "reports_D1" / "groups"
SITE_LEVEL_FILE = "site_level_details.csv"
SITE_LIST_SUFFIX = "webrtc_any_country_group_a_sites.txt"
REPORT_SUFFIX = "webrtc_any_country_focus_report.txt"
DEFAULT_GROUP_LABEL = "ALGERIA|INDIA|UAE__vs__GERMANY|USA"


def parse_group_label(label: str):
    if "__vs__" not in label:
        raise ValueError(
            "Group label must follow the pattern 'COUNTRY1|COUNTRY2__vs__COUNTRY3|COUNTRY4'"
        )
    left, right = label.split("__vs__", 1)
    group_a = tuple(country.strip() for country in left.split("|") if country.strip())
    group_b = tuple(country.strip() for country in right.split("|") if country.strip())
    if not group_a or not group_b:
        raise ValueError(f"Invalid group label: {label}")
    return group_a, group_b


def get_any_group_a_webrtc_sites(site_fingerprinting, group_a, group_b):
    webrtc_rows = site_fingerprinting[site_fingerprinting["fp_type"] == "webrtc"].copy()
    if webrtc_rows.empty:
        return []

    group_a_set = set(group_a)
    group_b_set = set(group_b)
    by_etld = webrtc_rows.groupby("etld")["location"].agg(set)

    selected = [
        str(etld)
        for etld, location_set in by_etld.items()
        if (location_set & group_a_set) and not (location_set & group_b_set)
    ]
    return sorted(selected)


def intersect_across_present_group_a(site_fingerprinting, etld, group_a, field_name):
    present_countries = set(
        site_fingerprinting[
            (site_fingerprinting["etld"] == etld)
            & (site_fingerprinting["fp_type"] == "webrtc")
            & (site_fingerprinting["location"].isin(group_a))
            & (site_fingerprinting[field_name].notna())
        ]["location"].astype(str).tolist()
    )

    if not present_countries:
        return set()

    value_sets = []
    for country in sorted(present_countries):
        values = set(
            site_fingerprinting[
                (site_fingerprinting["etld"] == etld)
                & (site_fingerprinting["location"] == country)
                & (site_fingerprinting["fp_type"] == "webrtc")
                & (site_fingerprinting[field_name].notna())
            ][field_name].astype(str).tolist()
        )
        value_sets.append(values)

    if not value_sets:
        return set()
    if len(value_sets) == 1:
        return value_sets[0]
    return set.intersection(*value_sets)


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


def write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_group_report(group_label):
    group_a, group_b = parse_group_label(group_label)
    group_dir = OUTPUT_ROOT / group_label
    site_level_path = group_dir / SITE_LEVEL_FILE

    site_level_df = pd.read_csv(site_level_path)
    site_level_df = site_level_df[(site_level_df["group_a"] == "|".join(group_a)) & (site_level_df["group_b"] == "|".join(group_b))].copy()
    site_fingerprinting = site_level_df[site_level_df["fp_type"].notna()].copy()

    selected_sites = get_any_group_a_webrtc_sites(site_fingerprinting, group_a, group_b)
    site_list_path = group_dir / SITE_LIST_SUFFIX
    write_text(site_list_path, "\n".join(selected_sites) + ("\n" if selected_sites else ""))

    common_domains_by_site = {}
    common_urls_by_site = {}
    for etld in selected_sites:
        common_domains = intersect_across_present_group_a(site_fingerprinting, etld, group_a, "fingerprinter")
        common_urls = intersect_across_present_group_a(site_fingerprinting, etld, group_a, "fingerprinting_url")
        if common_domains:
            common_domains_by_site[etld] = common_domains
        if common_urls:
            common_urls_by_site[etld] = common_urls

    total_selected_sites = len(selected_sites)

    report_lines = []
    report_lines.append(f"Group A: {'|'.join(group_a)}")
    report_lines.append(f"Group B: {'|'.join(group_b)}")
    report_lines.append(f"Group label: {group_label}")
    report_lines.append(f"WebRTC sites in any A country and no B countries: {len(selected_sites)}")
    report_lines.append(f"Sites with non-empty domain intersection across present A countries: {len(common_domains_by_site)}")
    report_lines.append(f"Sites with non-empty URL intersection across present A countries: {len(common_urls_by_site)}")
    report_lines.append("")
    report_lines.append("List of qualifying group-A WebRTC sites:")
    for site in selected_sites:
        report_lines.append(f"- {site}")
    report_lines.append("")

    if common_domains_by_site:
        report_lines.append("Distribution of shared WebRTC fingerprinting domains across the A countries where the site appears:")
        report_lines.extend(summarize_distribution(common_domains_by_site, total_selected_sites))
        report_lines.append("")
    else:
        report_lines.append("No site had a non-empty intersection for WebRTC fingerprinting domains across the relevant A countries.")
        report_lines.append("")

    if common_urls_by_site:
        report_lines.append("Distribution of shared WebRTC fingerprinting URLs across the A countries where the site appears:")
        report_lines.extend(summarize_distribution(common_urls_by_site, total_selected_sites))
    else:
        report_lines.append("No site had a non-empty intersection for WebRTC fingerprinting URLs across the relevant A countries.")

    report_path = group_dir / REPORT_SUFFIX
    write_text(report_path, "\n".join(report_lines) + "\n")

    return {
        "group_label": group_label,
        "group_dir": str(group_dir),
        "sites_file": str(site_list_path),
        "report_file": str(report_path),
        "n_selected_sites": len(selected_sites),
        "n_domain_intersection_sites": len(common_domains_by_site),
        "n_url_intersection_sites": len(common_urls_by_site),
    }


def main():
    result = build_group_report(DEFAULT_GROUP_LABEL)
    print(f"Generated site list: {result['sites_file']}")
    print(f"Generated report: {result['report_file']}")
    print(f"n_selected_sites: {result['n_selected_sites']}")
    print(f"n_domain_intersection_sites: {result['n_domain_intersection_sites']}")
    print(f"n_url_intersection_sites: {result['n_url_intersection_sites']}")


if __name__ == "__main__":
    main()
