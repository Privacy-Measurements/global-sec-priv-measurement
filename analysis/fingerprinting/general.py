import os
import json
from collections import defaultdict
from urllib.parse import urlparse
import pandas as pd
import tldextract
from dotenv import load_dotenv
from sqlalchemy import create_engine


OUTPUT_DIR = "reports_general"
TOP_K = 10
GROUP_COLS = ["etld", "category", "location"]

QUERY = """
SELECT
    etld,
    url,
    category,
    location,
    fingerprinting,
    is_fingerprinting
FROM crawl_sessions
WHERE category NOT LIKE '%%validation'
  AND location NOT LIKE '%%VPN'
  AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
  AND (is_cloudflare IS NULL OR is_cloudflare = 0)
"""


def is_valid_url(u: str) -> bool:
    try:
        return urlparse(u).scheme in {"http", "https"}
    except Exception:
        return False


def url_path(caller_url: str) -> str:
    """
    Returns the path part of a URL (everything after the domain).
    """
    try:
        parsed = urlparse(caller_url)
        return parsed.path
    except Exception:
        return None

def full_domain(caller_url: str) -> str:
    try:
        parsed = urlparse(caller_url)
        return parsed.hostname
    except Exception:
        return None


def etld_plus_one(u: str) -> str | None:
    if not u:
        return None
        
    if not is_valid_url(u):
        return None

    ext = tldextract.extract(u)
    return ext.top_domain_under_public_suffix

def load_data() -> pd.DataFrame:
    load_dotenv()

    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}/"
        f"{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )

    df = pd.read_sql(QUERY, engine)

    df["fingerprinting"] = df["fingerprinting"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else {}
    )

    return df

def compute_fingerprinting_prevalence(df: pd.DataFrame) -> None:
    fp_flag = (
        df.groupby(GROUP_COLS)["is_fingerprinting"]
        .max()
        .reset_index(name="has_fp")
    )

    prevalence = (
        fp_flag.groupby(["category", "location"])["has_fp"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
    )

    prevalence.to_csv(f"{OUTPUT_DIR}/fingerprinting_prevalence.csv")


def compute_type_prevalence(df: pd.DataFrame) -> None:
    fp_types = set()
    for v in df["fingerprinting"]:
        fp_types.update(v.keys())

    for fp_type in sorted(fp_types):
        tmp = df.copy()

        # mark rows that have this fingerprinting type
        tmp["has_fp_type"] = tmp["fingerprinting"].apply(
            lambda d: 1 if d[fp_type] else 0
        )

        # collapse to (etld, category, location)
        collapsed = (
            tmp.groupby(GROUP_COLS)["has_fp_type"]
            .max()
            .reset_index()
        )

        # compute prevalence
        prevalence = (
            collapsed.groupby(["category", "location"])["has_fp_type"]
            .mean()
            .unstack()
            .multiply(100)
            .round(2)
        )

        prevalence.to_csv(
            f"{OUTPUT_DIR}/fingerprinting_prevalence_{fp_type}.csv"
        )

# ====================
# FP / TP CLASSIFICATION
# ====================
def build_fp_instance_table(df: pd.DataFrame) -> pd.DataFrame:
    records = []

    for _, row in df.iterrows():
        page_etld = etld_plus_one(row["url"])

        for fp_type, entries in row["fingerprinting"].items():

            for e in entries:
                
                if 'caller_url' in e:
                    caller = e["caller_url"]
                elif 'script_src' in e:
                    caller = e["script_src"]
                else:
                    raise KeyError("No caller_url or script_src in fingerprinting entry")

                if not is_valid_url(caller):
                    party = "first"
                else:
                    party = (
                        "first"
                        if etld_plus_one(caller) == page_etld
                        else "third"
                    )

                records.append({
                    "category": row["category"],
                    "location": row["location"],
                    "etld": row["etld"],
                    "fp_type": fp_type,
                    "caller_url": caller,
                    "party": party
                })

    res = pd.DataFrame(records)
    print('number of FP records:', res.shape[0])

    res = res.drop_duplicates()
    print('number of unique FP records:', res.shape[0])

    return res

# ====================
# FP / TP (ETLD LEVEL)
# ====================
def compute_fp_tp_prevalence(df: pd.DataFrame, fp_df: pd.DataFrame) -> None:
    """
    Computes etld-level prevalence of first-party and third-party fingerprinting.
    Denominator = all etlds per (category, location).
    """

    all_etlds = (
        df[GROUP_COLS]
        .drop_duplicates()
        .assign(_dummy=True)
    )

    fp_presence = (
        fp_df.groupby(GROUP_COLS + ["party"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    fp_presence["has_first"] = fp_presence["first"] > 0
    fp_presence["has_third"] = fp_presence["third"] > 0

    # Merge with full etld universe
    fp_presence = all_etlds.merge(
        fp_presence, on=GROUP_COLS, how="left"
    )

    fp_presence["has_first"] = (
        fp_presence["first"].fillna(0).astype(int) > 0
    )

    fp_presence["has_third"] = (
        fp_presence["third"].fillna(0).astype(int) > 0
    )

    # Compute prevalence using correct denominator
    for party, col in [("first", "has_first"), ("third", "has_third")]:
        tbl = (
            fp_presence.groupby(["category", "location"])[col]
            .mean()
            .unstack()
            .multiply(100)
            .round(2)
        )

        tbl.to_csv(
            f"{OUTPUT_DIR}/{party}_party_fingerprinting_prevalence.csv"
        )

# ====================
# FP / TP (INSTANCE LEVEL)
# ====================
def compute_fp_tp_instance_distribution(fp_df: pd.DataFrame) -> None:
    inst = (
        fp_df.groupby(["category", "location", "party"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    inst["total"] = inst["first"] + inst["third"]

    inst["first_party_pct"] = (
        inst["first"] / inst["total"] * 100
    ).round(2)

    inst["third_party_pct"] = (
        inst["third"] / inst["total"] * 100
    ).round(2)

    inst[
        ["category", "location", "first_party_pct", "third_party_pct", "total"]
    ].to_csv(
        f"{OUTPUT_DIR}/third_party_first_party_fingerprinting_instance_distribution.csv",
        index=False,
    )



# ====================
# TOP FINGERPRINTERS
# ====================
def compute_top_fingerprinters(df: pd.DataFrame, fp_df: pd.DataFrame, party: str, mode: str = "full") -> None:
    tmp = fp_df[fp_df["party"] == party]


    denominators = (
        df[["category", "location", "etld"]]
        .drop_duplicates()
        .groupby(["category", "location"])
        .size()
        .to_dict()
    )

    seen = set()
    counts = defaultdict(set)


    for _, r in tmp.iterrows():

        if mode == "full":
            caller_id = full_domain(r["caller_url"])
        elif mode == "etld":
            caller_id = etld_plus_one(r["caller_url"])
        elif mode == "path":
            caller_id = url_path(r["caller_url"])
        else:
            raise ValueError(f"Unknown mode: {mode}")

        key = (r["category"], r["location"], r["etld"], caller_id)


        if key not in seen:
            counts[(r["category"], r["location"], caller_id)].add(r["etld"])
            seen.add(key)


    rows = []
    for (cat, loc, caller), etlds in counts.items():
        denom = denominators.get((cat, loc), 0)
        if denom == 0:
            continue

        pct = (len(etlds) / denom) * 100

        rows.append({
            "column": f"{cat}-{loc}",
            "caller_url": caller,
            "percentage": round(pct, 2),
        })

    df = pd.DataFrame(rows)
    result = {}

    for col, g in df.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.caller_url} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    out = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()]))


    out.to_csv(
        f"{OUTPUT_DIR}/top_{party}_party_fingerprinters_{mode}.csv",
        index=False,
    )


def compute_top_fingerprinters_country_level(df: pd.DataFrame, fp_df: pd.DataFrame, party: str, mode: str = "full") -> None:
    tmp = fp_df[fp_df["party"] == party]

    # Denominator: all sites per country (across all categories)
    denominators = (
        df[["location", "etld"]]
        .drop_duplicates()
        .groupby("location")
        .size()
        .to_dict()
    )

    seen = set()
    counts = defaultdict(set)

    for _, r in tmp.iterrows():

        if mode == "full":
            caller_id = full_domain(r["caller_url"])
        elif mode == "etld":
            caller_id = etld_plus_one(r["caller_url"])
        elif mode == "path":
            caller_id = url_path(r["caller_url"])
        else:
            raise ValueError(f"Unknown mode: {mode}")

        key = (r["location"], r["etld"], caller_id)

        if key not in seen:
            counts[(r["location"], caller_id)].add(r["etld"])
            seen.add(key)

    rows = []
    for (loc, caller), etlds in counts.items():
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue

        pct = (len(etlds) / denom) * 100

        rows.append({
            "column": loc,
            "caller_url": caller,
            "percentage": round(pct, 2),
        })

    df_out = pd.DataFrame(rows)
    result = {}

    for col, g in df_out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.caller_url} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    out = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()]))

    out.to_csv(
        f"{OUTPUT_DIR}/top_{party}_party_fingerprinters_{mode}_country_level.csv",
        index=False,
    )


def compute_country_specific_third_party_fingerprinters(
    df: pd.DataFrame,
    fp_df: pd.DataFrame,
    mode: str = "full",   # "full" or "etld"
):
    """
    Computes country-specific third-party fingerprinters and
    summarizes how much of fingerprinting they account for.
    """

    tmp = fp_df[fp_df["party"] == "third"].copy()

    # Choose identifier
    if mode == "full":
        tmp["fp_id"] = tmp["caller_url"].apply(full_domain)
    elif mode == "etld":
        tmp["fp_id"] = tmp["caller_url"].apply(etld_plus_one)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    tmp = tmp.dropna(subset=["fp_id"])

    # All third-party fingerprinting per country (denominator)
    total_fp_per_country = (
        tmp.groupby(["location", "fp_id", "etld"])
        .size()
        .reset_index()
        .groupby("location")
        .size()
        .to_dict()
    )

    # Presence: (location, fp_id) -> set(etlds)
    presence = defaultdict(set)
    seen = set()

    for _, r in tmp.iterrows():
        key = (r["location"], r["etld"], r["fp_id"])
        if key not in seen:
            presence[(r["location"], r["fp_id"])].add(r["etld"])
            seen.add(key)

    # fp_id -> set(locations)
    fp_locations = defaultdict(set)
    for (loc, fp_id) in presence:
        fp_locations[fp_id].add(loc)

    # Collect country-specific stats
    top_rows = []
    summary_rows = []

    for loc in tmp["location"].unique():
        # country-specific fingerprinters in this country
        local_fps = {
            fp_id for (l, fp_id) in presence
            if l == loc and len(fp_locations[fp_id]) == 1
        }

        # number of distinct country-specific fingerprinters
        num_specific = len(local_fps)

        # how many fingerprinting occurrences they account for
        specific_occurrences = sum(
            len(presence[(loc, fp_id)])
            for fp_id in local_fps
            if (loc, fp_id) in presence
        )

        total_occurrences = total_fp_per_country.get(loc, 0)

        coverage_pct = (
            (specific_occurrences / total_occurrences) * 100
            if total_occurrences > 0 else 0
        )

        summary_rows.append({
            "country": loc,
            "num_country_specific_fingerprinters": num_specific,
            "coverage_pct": round(coverage_pct, 2),
        })

        # prepare top-10 table
        for fp_id in local_fps:
            pct = (
                (len(presence[(loc, fp_id)]) / total_occurrences) * 100
                if total_occurrences > 0 else 0
            )
            top_rows.append({
                "column": loc,
                "fingerprinter": fp_id,
                "percentage": round(pct, 2),
            })

    # ---- Top-10 table ----
    top_df = pd.DataFrame(top_rows)
    top_out = {}

    for col, g in top_df.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        top_out[col] = [
            f"{r.fingerprinter} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in top_out.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/country_specific_third_party_fingerprinters_{mode}.csv",
            index=False,
        )

    # ---- Summary table ----
    pd.DataFrame(summary_rows).to_csv(
        f"{OUTPUT_DIR}/country_specific_third_party_fingerprinters_{mode}_summary.csv",
        index=False,
    )

def compute_number_of_third_party_fingerprinters(fp_df: pd.DataFrame) -> None:
    """
    Computes, for each (category, location), the number of distinct
    third-party fingerprinting domains (full domain, incl. subdomains).

    Output:
      rows    = categories
      columns = locations (countries)
      values  = # distinct third-party fingerprinting domains
    """

    # keep only third-party fingerprinting
    tmp = fp_df[fp_df["party"] == "third"].copy()

    # extract full domain (incl. subdomains)
    tmp["fp_domain"] = tmp["caller_url"].apply(full_domain)

    # drop invalid / missing domains
    tmp = tmp.dropna(subset=["fp_domain"])

    # count distinct domains per (category, location)
    result = (
        tmp.groupby(["category", "location"])["fp_domain"]
        .nunique()
        .unstack(fill_value=0)
        .sort_index()
    )

    result.to_csv(
        f"{OUTPUT_DIR}/number_of_third_party_fingerprinters.csv"
    )

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = load_data()

    compute_fingerprinting_prevalence(df)
    compute_type_prevalence(df)

    fp_df = build_fp_instance_table(df)

    compute_fp_tp_prevalence(df, fp_df)
    compute_fp_tp_instance_distribution(fp_df)


    compute_top_fingerprinters(df, fp_df, party="first", mode="path")
    compute_top_fingerprinters_country_level(df, fp_df, party="first", mode="path")



    # third-party – full domain (with subdomains)
    compute_top_fingerprinters(df, fp_df, party="third", mode="full")
    compute_top_fingerprinters_country_level(df, fp_df, party="third", mode="full")

    # third-party – eTLD+1
    compute_top_fingerprinters(df, fp_df, party="third", mode="etld")
    compute_top_fingerprinters_country_level(df, fp_df, party="third", mode="etld")


    compute_country_specific_third_party_fingerprinters(df, fp_df, mode="full")
    compute_country_specific_third_party_fingerprinters(df, fp_df, mode="etld")


    compute_number_of_third_party_fingerprinters(fp_df)


if __name__ == "__main__":
    main()
