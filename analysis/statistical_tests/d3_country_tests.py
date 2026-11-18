import csv
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy.stats import chi2_contingency, kruskal
from sqlalchemy import create_engine
from statsmodels.stats.proportion import proportion_confint

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

COUNTRIES = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
OUTPUT_DIR = Path(__file__).resolve().parent / "D3"
OUTPUT_FILE = OUTPUT_DIR / "d3_country_tests_summary.csv"
RESULTS_FILE = OUTPUT_DIR / "d3_country_tests_results.csv"


def get_engine():
    load_dotenv(REPO_ROOT / ".env")
    return create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )


def load_total_sites_by_country(engine):
    df = pd.read_sql(
        """
        SELECT etld, location
        FROM crawl_sessions
        WHERE location NOT LIKE '%%VPN'
          AND category = 'country_specific'
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
        """,
        engine,
    )
    return df.groupby("location")["etld"].nunique().to_dict()


def load_positive_sites_by_country(csv_path):
    counts = {country: set() for country in COUNTRIES}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("category") != "country_specific":
                continue
            country = row.get("location", "").upper()
            if country in counts:
                counts[country].add(row.get("etld", ""))
    return counts


def load_tracker_count_samples_by_country(csv_path):
    samples = {country: [] for country in COUNTRIES}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        by_country_site = {country: {} for country in COUNTRIES}
        for row in reader:
            if row.get("category") != "country_specific":
                continue
            country = row.get("location", "").upper()
            if country not in by_country_site:
                continue
            etld = row.get("etld", "")
            by_country_site[country].setdefault(etld, set()).add(row.get("tracker_domain", ""))
        for country in COUNTRIES:
            samples[country] = [len(trackers) for trackers in by_country_site[country].values()]
    return samples


def build_tracking_stats(engine):
    total_sites = load_total_sites_by_country(engine)
    positive_sites = load_positive_sites_by_country(
        REPO_ROOT / "analysis" / "tracking" / "reports_tracking" / "site_level_tracking.csv"
    )
    tracker_counts = load_tracker_count_samples_by_country(
        REPO_ROOT / "analysis" / "tracking" / "reports_tracking" / "site_level_tracking.csv"
    )
    stats = {}
    for country in COUNTRIES:
        n = int(total_sites.get(country, 0))
        k = len(positive_sites.get(country, set()))
        stats[country] = {
            "total_sites": n,
            "with_tracking": k,
            "prevalence": k / n if n else 0.0,
            "tracker_counts": tracker_counts.get(country, []),
        }
    return stats


def build_fingerprinting_stats(engine):
    total_sites = load_total_sites_by_country(engine)
    positive_sites = load_positive_sites_by_country(
        REPO_ROOT / "analysis" / "fingerprinting" / "reports_general" / "site_level_summary.csv"
    )
    stats = {}
    for country in COUNTRIES:
        n = int(total_sites.get(country, 0))
        k = len(positive_sites.get(country, set()))
        stats[country] = {
            "total_sites": n,
            "with_fingerprinting": k,
            "prevalence": k / n if n else 0.0,
        }
    return stats


def confidence_interval(k, n):
    """95% Wilson score interval for k successes out of n."""
    if n <= 0:
        return 0.0, 0.0
    return proportion_confint(k, n, alpha=0.05, method="wilson")


def run_prevalence_test(stats, key):
    observed = [
        [stats[c][key], stats[c]["total_sites"] - stats[c][key]]
        for c in COUNTRIES
    ]
    chi2, p_value, dof, _ = chi2_contingency(observed)
    n = sum(stats[c]["total_sites"] for c in COUNTRIES)
    k = len(COUNTRIES)
    cramers_v = (chi2 / (n * (k - 1))) ** 0.5
    return chi2, p_value, dof, cramers_v


def run_kw_tracker_count_test(tracking_stats):
    samples = [tracking_stats[c]["tracker_counts"] for c in COUNTRIES]
    if any(len(sample) == 0 for sample in samples):
        raise ValueError("At least one D3 country has no tracker-count sample.")
    h, p_value = kruskal(*samples)
    n = sum(len(s) for s in samples)
    epsilon_sq = h / (n - 1)
    return h, p_value, len(samples) - 1, epsilon_sq


def tracker_count_summary(sample):
    s = np.asarray(sample)
    if s.size == 0:
        return 0.0, 0.0, 0.0, 0.0
    return float(np.median(s)), float(np.percentile(s, 25)), float(np.percentile(s, 75)), float(s.mean())


def write_outputs(tracking_stats, fp_stats, tr, fp, kw):
    tracking_chi2, tracking_p, tracking_dof, tracking_v = tr
    fp_chi2, fp_p, fp_dof, fp_v = fp
    kw_h, kw_p, kw_dof, kw_eps = kw

    rows = []
    for country in COUNTRIES:
        n = tracking_stats[country]["total_sites"]
        kt = tracking_stats[country]["with_tracking"]
        kf = fp_stats[country]["with_fingerprinting"]
        t_lo, t_hi = confidence_interval(kt, n)
        f_lo, f_hi = confidence_interval(kf, n)
        med, q1, q3, mean = tracker_count_summary(tracking_stats[country]["tracker_counts"])
        rows.append(
            {
                "country": country,
                "tracking_prevalence": kt / n if n else 0.0,
                "tracking_ci_low": t_lo,
                "tracking_ci_high": t_hi,
                "fingerprinting_prevalence": kf / n if n else 0.0,
                "fingerprinting_ci_low": f_lo,
                "fingerprinting_ci_high": f_hi,
                "trackers_per_site_median": med,
                "trackers_per_site_q1": q1,
                "trackers_per_site_q3": q3,
                "trackers_per_site_mean": mean,
            }
        )
    summary = pd.DataFrame(rows)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_FILE, index=False)

    pd.DataFrame(
        [
            {"test": "tracking_prevalence_chi2", "statistic": tracking_chi2, "dof": tracking_dof,
             "p_value": tracking_p, "effect_size": tracking_v, "effect_size_name": "cramers_v"},
            {"test": "fingerprinting_prevalence_chi2", "statistic": fp_chi2, "dof": fp_dof,
             "p_value": fp_p, "effect_size": fp_v, "effect_size_name": "cramers_v"},
            {"test": "tracker_count_kruskal_wallis", "statistic": kw_h, "dof": kw_dof,
             "p_value": kw_p, "effect_size": kw_eps, "effect_size_name": "epsilon_squared"},
        ]
    ).to_csv(RESULTS_FILE, index=False)
    return summary


def main():
    engine = get_engine()
    tracking_stats = build_tracking_stats(engine)
    fp_stats = build_fingerprinting_stats(engine)

    tr = run_prevalence_test(tracking_stats, "with_tracking")
    fp = run_prevalence_test(fp_stats, "with_fingerprinting")
    kw = run_kw_tracker_count_test(tracking_stats)

    summary = write_outputs(tracking_stats, fp_stats, tr, fp, kw)

    print("D3 country statistical tests")
    print(f"Tracking prevalence:       chi2({tr[2]})={tr[0]:.4f}, p={tr[1]:.3e}, Cramer's V={tr[3]:.3f}")
    print(f"Fingerprinting prevalence: chi2({fp[2]})={fp[0]:.4f}, p={fp[1]:.3e}, Cramer's V={fp[3]:.3f}")
    print(f"Tracker count distribution: H({kw[2]})={kw[0]:.4f}, p={kw[1]:.3e}, epsilon^2={kw[3]:.4f}")
    print()
    print(summary.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print()
    print(f"Saved summary to {OUTPUT_FILE}")
    print(f"Saved statistics to {RESULTS_FILE}")


if __name__ == "__main__":
    main()