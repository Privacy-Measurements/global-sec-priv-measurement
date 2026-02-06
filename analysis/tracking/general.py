import os
from collections import defaultdict
from urllib.parse import urlparse

import pandas as pd
import tldextract
from dotenv import load_dotenv
from sqlalchemy import create_engine


# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = "reports_tracking"
TOP_K = 10
GROUP_COLS = ["etld", "category", "location"]

os.makedirs(OUTPUT_DIR, exist_ok=True)



# ============================================================
# DATA LOADING
# ============================================================

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
            location
        FROM crawl_sessions
        WHERE category NOT LIKE '%%validation'
          AND location NOT LIKE '%%VPN'
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
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
            WHERE category NOT LIKE '%%validation'
                AND location NOT LIKE '%%VPN'
                AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
                AND (is_cloudflare IS NULL OR is_cloudflare = 0)
        )
    """, engine)

    return sessions, requests


from urllib.parse import urlparse
import tldextract

def extract_tracker_domain(request_url: str) -> str | None:
    try:
        hostname = urlparse(request_url).hostname
        if not hostname:
            return None
        ext = tldextract.extract(hostname)
        if not ext.domain or not ext.suffix:
            return None
        return f"{ext.domain}.{ext.suffix}"
    except Exception:
        return None

'''        
def extract_tracker_domain(request_url: str) -> str | None:
    try:
        return urlparse(request_url).hostname
    except Exception:
        return None
'''

def build_site_tracking_table(sessions: pd.DataFrame, requests: pd.DataFrame) -> pd.DataFrame:
    """
    One row = (etld, category, location, tracker_domain)
    """

    df = requests.merge(
        sessions,
        on="session_id",
        how="inner",
    )

    df["tracker_domain"] = df["request_url"].apply(extract_tracker_domain)
    df = df.dropna(subset=["tracker_domain"])

    # Reduce to site-level presence
    df = df.drop_duplicates(
        subset=["etld", "category", "location", "tracker_domain"]
    )

    return df[["etld", "category", "location", "tracker_domain"]]


def compute_tracking_prevalence(sessions: pd.DataFrame, site_tracking: pd.DataFrame) -> None:
    """
    Percentage of etlds with at least one third-party tracker.
    """

    all_etlds = (
        sessions[["etld", "category", "location"]]
        .drop_duplicates()
    )

    tracked_etlds = (
        site_tracking[["etld", "category", "location"]]
        .drop_duplicates()
        .assign(has_tracking=1)
    )

    merged = all_etlds.merge(
        tracked_etlds,
        on=["etld", "category", "location"],
        how="left",
    ).fillna({"has_tracking": 0})

    prevalence = (
        merged.groupby(["category", "location"])["has_tracking"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
    )

    prevalence.to_csv(
        f"{OUTPUT_DIR}/tracking_prevalence.csv"
    )

def compute_top_tracking_domains(sessions: pd.DataFrame, site_tracking: pd.DataFrame) -> None:

    denominators = (
        sessions[["category", "location", "etld"]]
        .drop_duplicates()
        .groupby(["category", "location"])
        .size()
        .to_dict()
    )

    presence = defaultdict(set)

    for _, r in site_tracking.iterrows():
        presence[
            (r["category"], r["location"], r["tracker_domain"])
        ].add(r["etld"])

    rows = []
    for (cat, loc, dom), etlds in presence.items():
        denom = denominators.get((cat, loc), 0)
        if denom == 0:
            continue

        rows.append({
            "column": f"{cat}-{loc}",
            "tracker": dom,
            "percentage": round(len(etlds) / denom * 100, 2),
        })

    df = pd.DataFrame(rows)
    out = {}

    for col, g in df.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        out[col] = [
            f"{r.tracker} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in out.items()])) \
        .to_csv(f"{OUTPUT_DIR}/top_tracking_domains.csv", index=False)


def compute_country_specific_tracking_domains(site_tracking: pd.DataFrame) -> None:

    presence = defaultdict(set)
    for _, r in site_tracking.iterrows():
        presence[(r["location"], r["tracker_domain"])].add(r["etld"])

    tracker_locations = defaultdict(set)
    for (loc, dom) in presence:
        tracker_locations[dom].add(loc)

    total_occurrences = (
        site_tracking
        .groupby(["location", "tracker_domain", "etld"])
        .size()
        .reset_index()
        .groupby("location")
        .size()
        .to_dict()
    )

    summary = []

    for loc in site_tracking["location"].unique():
        local_only = {
            dom for (l, dom) in presence
            if l == loc and len(tracker_locations[dom]) == 1
        }

        occ = sum(len(presence[(loc, dom)]) for dom in local_only)
        total = total_occurrences.get(loc, 0)

        summary.append({
            "country": loc,
            "num_country_specific_trackers": len(local_only),
            "coverage_pct": round(
                occ / total * 100 if total > 0 else 0, 2
            ),
        })

    pd.DataFrame(summary).to_csv(
        f"{OUTPUT_DIR}/country_specific_tracking_domains_summary.csv",
        index=False,
    )


def compute_avg_trackers_per_site(site_tracking: pd.DataFrame) -> None:
    """
    Average number of distinct third-party trackers per etld,
    per (category, location).
    """

    per_site = (
        site_tracking
        .groupby(["category", "location", "etld"])["tracker_domain"]
        .nunique()
        .reset_index(name="num_trackers")
    )

    avg = (
        per_site
        .groupby(["category", "location"])["num_trackers"]
        .mean()
        .unstack()
        .round(2)
    )

    avg.to_csv(
        f"{OUTPUT_DIR}/avg_num_third_party_trackers.csv"
    )



def compute_total_distinct_trackers(site_tracking: pd.DataFrame) -> None:
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

    total.to_csv(
        f"{OUTPUT_DIR}/total_num_third_party_trackers.csv"
    )




def compute_top_country_specific_tracking_domains(
    site_tracking: pd.DataFrame,
) -> None:
    """
    Top country-specific third-party tracking domains per country.
    """

    # (location, tracker_domain) -> set(etlds)
    presence = defaultdict(set)
    for _, r in site_tracking.iterrows():
        presence[(r["location"], r["tracker_domain"])].add(r["etld"])

    # tracker_domain -> set(locations)
    tracker_locations = defaultdict(set)
    for (loc, dom) in presence:
        tracker_locations[dom].add(loc)

    rows = []

    for (loc, dom), etlds in presence.items():
        if len(tracker_locations[dom]) == 1:
            rows.append({
                "column": loc,
                "tracker": dom,
                "etld_count": len(etlds),
            })

    df = pd.DataFrame(rows)
    out = {}

    for col, g in df.groupby("column"):
        g = g.sort_values("etld_count", ascending=False).head(TOP_K)
        out[col] = [
            f"{r.tracker} ({r.etld_count})"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in out.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/top_country_specific_tracking_domains.csv",
            index=False,
        )



def main():
    sessions, requests = load_tables()

    site_tracking = build_site_tracking_table(
        sessions, requests
    )

    compute_tracking_prevalence(sessions, site_tracking)
    compute_top_tracking_domains(sessions, site_tracking)
    compute_country_specific_tracking_domains(site_tracking)


    compute_avg_trackers_per_site(site_tracking)
    compute_total_distinct_trackers(site_tracking)
    compute_top_country_specific_tracking_domains(site_tracking)


if __name__ == "__main__":
    main()