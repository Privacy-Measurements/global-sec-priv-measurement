import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy.stats import friedmanchisquare
from sqlalchemy import create_engine
from statsmodels.stats.contingency_tables import cochrans_q
from statsmodels.stats.proportion import proportion_confint

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

COUNTRIES = ["ALGERIA", "GERMANY", "INDIA", "UAE", "USA"]
OUTPUT_DIR = Path(__file__).resolve().parent / "D1"
TRACKING_CSV = REPO_ROOT / "analysis" / "tracking" / "reports_tracking" / "site_level_tracking.csv"
FINGERPRINTING_CSV = REPO_ROOT / "analysis" / "fingerprinting" / "reports_general" / "site_level_summary.csv"
SUMMARY_FILE = OUTPUT_DIR / "d1_country_tests_summary.csv"
RESULTS_FILE = OUTPUT_DIR / "d1_country_tests_results.csv"


def get_engine():
    load_dotenv(REPO_ROOT / ".env")
    return create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )


def load_common_global_etlds(engine):
    df = pd.read_sql(
        """
        SELECT etld, location
        FROM crawl_sessions
        WHERE category = 'global'
          AND location NOT LIKE '%%VPN'
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
        """,
        engine,
    )
    common = df.groupby("etld")["location"].apply(set).to_dict()
    return sorted(etld for etld, locations in common.items() if locations == set(COUNTRIES))


def load_country_tracker_counts(common_sites, tracking_path):
    tracking = pd.read_csv(tracking_path)
    tracking = tracking[tracking["category"].eq("global")].copy()
    tracking["location"] = tracking["location"].astype(str).str.upper()
    site_counts = (
        tracking.groupby(["location", "etld"], as_index=False)["tracker_domain"]
        .nunique()
        .rename(columns={"tracker_domain": "num_trackers"})
    )
    counts_by_country = {}
    for country in COUNTRIES:
        cc = site_counts[site_counts["location"].eq(country)].set_index("etld")["num_trackers"]
        counts_by_country[country] = [int(cc.get(etld, 0)) for etld in common_sites]
    return counts_by_country


def load_country_fingerprinting_presence(common_sites, fingerprinting_path):
    fp = pd.read_csv(fingerprinting_path)
    fp = fp[fp["category"].eq("global")].copy()
    fp["location"] = fp["location"].astype(str).str.upper()
    site_presence = (
        fp.groupby(["location", "etld"], as_index=False)["fingerprinter"]
        .nunique()
        .rename(columns={"fingerprinter": "num_fingerprinters"})
    )
    presence_by_country = {}
    for country in COUNTRIES:
        cp = site_presence[site_presence["location"].eq(country)].set_index("etld")["num_fingerprinters"]
        presence_by_country[country] = [1 if int(cp.get(etld, 0)) > 0 else 0 for etld in common_sites]
    return presence_by_country


def wilson_ci(k, n):
    """95% Wilson score interval for k successes out of n."""
    if n <= 0:
        return 0.0, 0.0
    return proportion_confint(k, n, alpha=0.05, method="wilson")


def run_cochran_q(binary_by_country):
    """Cochran's Q for k paired binary samples. Rows = sites, columns = countries."""
    x = np.array([binary_by_country[c] for c in COUNTRIES]).T  # shape (n_sites, k)
    res = cochrans_q(x, return_object=True)
    return float(res.statistic), float(res.pvalue), int(res.df)


def run_friedman(counts_by_country):
    samples = [counts_by_country[c] for c in COUNTRIES]
    stat, p = friedmanchisquare(*samples)
    n = len(samples[0])
    k = len(samples)
    kendall_w = stat / (n * (k - 1))
    return float(stat), float(p), k - 1, float(kendall_w)


def main():
    engine = get_engine()
    common_sites = load_common_global_etlds(engine)
    n = len(common_sites)

    tracking_counts = load_country_tracker_counts(common_sites, TRACKING_CSV)
    tracking_presence = {c: [1 if v > 0 else 0 for v in tracking_counts[c]] for c in COUNTRIES}
    fp_presence = load_country_fingerprinting_presence(common_sites, FINGERPRINTING_CSV)

    tq, tp, tdf = run_cochran_q(tracking_presence)
    fq, fp_p, fdf = run_cochran_q(fp_presence)
    fr_stat, fr_p, fr_df, kendall_w = run_friedman(tracking_counts)

    rows = []
    for c in COUNTRIES:
        counts = np.array(tracking_counts[c])
        nonzero = counts[counts > 0]
        tracked = int(sum(tracking_presence[c]))
        fpn = int(sum(fp_presence[c]))
        t_lo, t_hi = wilson_ci(tracked, n)
        f_lo, f_hi = wilson_ci(fpn, n)
        rows.append({
            "country": c,
            "tracking_prevalence": tracked / n,
            "tracking_ci_low": t_lo,
            "tracking_ci_high": t_hi,
            "fingerprinting_prevalence": fpn / n,
            "fingerprinting_ci_low": f_lo,
            "fingerprinting_ci_high": f_hi,
            "trackers_per_tracking_site_mean": float(nonzero.mean()) if tracked else 0.0,
            "trackers_per_tracking_site_median": float(np.median(nonzero)) if tracked else 0.0,
            "trackers_per_tracking_site_q1": float(np.percentile(nonzero, 25)) if tracked else 0.0,
            "trackers_per_tracking_site_q3": float(np.percentile(nonzero, 75)) if tracked else 0.0,
            "trackers_per_site_mean_incl_zero": float(counts.mean()),
        })
    summary = pd.DataFrame(rows)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_csv(SUMMARY_FILE, index=False)

    tracking_range = summary["tracking_prevalence"].max() - summary["tracking_prevalence"].min()
    fp_range = summary["fingerprinting_prevalence"].max() - summary["fingerprinting_prevalence"].min()

    pd.DataFrame([
        {"test": "tracking_prevalence_cochran_q", "statistic": tq, "dof": tdf, "p_value": tp,
         "effect_size": tracking_range, "effect_size_name": "prevalence_range"},
        {"test": "fingerprinting_prevalence_cochran_q", "statistic": fq, "dof": fdf, "p_value": fp_p,
         "effect_size": fp_range, "effect_size_name": "prevalence_range"},
        {"test": "tracker_count_friedman", "statistic": fr_stat, "dof": fr_df, "p_value": fr_p,
         "effect_size": kendall_w, "effect_size_name": "kendall_w"},
    ]).to_csv(RESULTS_FILE, index=False)

    print(f"Common D1 sites (crawled from all five countries): {n}")
    print()
    print(summary.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print()
    print(f"Tracking prevalence:       Cochran's Q({tdf})={tq:.2f}, p={tp:.3e}, range={100*tracking_range:.1f} pp")
    print(f"Fingerprinting prevalence: Cochran's Q({fdf})={fq:.2f}, p={fp_p:.3e}, range={100*fp_range:.1f} pp")
    print(f"Tracker counts:            Friedman Q({fr_df})={fr_stat:.2f}, p={fr_p:.3e}, Kendall's W={kendall_w:.3f}")
    print(f"Saved summary to {SUMMARY_FILE}")
    print(f"Saved statistics to {RESULTS_FILE}")


if __name__ == "__main__":
    main()