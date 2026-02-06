import os
import json
from collections import defaultdict
from urllib.parse import urlparse
from tqdm import tqdm

import pandas as pd
import tldextract
from dotenv import load_dotenv
from sqlalchemy import create_engine
import hashlib
import base64
import time 

# ====================
# CONFIG
# ====================

OUTPUT_DIR = "reports_user_identification"
TOP_K = 10
GROUP_COLS = ["etld", "category", "location"]

UID_TYPES = {
    "cookies": "cookies_storage_identifiers",
    "local_storage": "local_storage_identifiers",
    "session_storage": "session_storage_identifiers",
}

UID_CONFIGS = {
#    "all": ["cookies", "local_storage", "session_storage"],
#    "cookies": ["cookies"],
#    "local_storage": ["local_storage"],
#    "session_storage": ["session_storage"],
    "cookies_local": ["cookies", "local_storage"],
}

QUERY = """
SELECT
    id,
    etld,
    url,
    category,
    location,
    user_identifiers
FROM crawl_sessions
WHERE category NOT LIKE '%%validation'
  AND location NOT LIKE '%%VPN'
  AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
  AND (is_cloudflare IS NULL OR is_cloudflare = 0)
"""

# ====================
# URL HELPERS
# ====================

def is_valid_url(u: str) -> bool:
    try:
        return urlparse(u).scheme in {"http", "https"}
    except Exception:
        return False

def full_domain(u: str):
    try:
        return urlparse(u).hostname
    except Exception:
        return None

def etld_plus_one(u: str):
    if not u or not is_valid_url(u):
        return None
    ext = tldextract.extract(u)
    return ext.top_domain_under_public_suffix

# ====================
# LOAD DATA
# ====================

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
    df["user_identifiers"] = df["user_identifiers"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else {}
    )
    return df

# ====================
# UID EXTRACTION
# ====================

def extract_uid_entries(uid_json: dict, uid_types: list[str]) -> list[dict]:
    entries = []
    for t in uid_types:
        entries.extend(uid_json.get(UID_TYPES[t], []))
    return entries

# ====================
# UID INSTANCE TABLE
# ====================

def build_uid_instance_table(df: pd.DataFrame, uid_types: list[str]) -> pd.DataFrame:
    records = []

    for _, row in df.iterrows():
        page_etld = etld_plus_one(row["url"])
        entries = extract_uid_entries(row["user_identifiers"], uid_types)

        for e in entries:
            caller = e.get("caller_url")

            if not is_valid_url(caller):
                party = "first"
            else:
                party = "first" if etld_plus_one(caller) == page_etld else "third"

            records.append({
                "category": row["category"],
                "location": row["location"],
                "etld": row["etld"],
                "caller_url": caller,
                "party": party,
            })

    uid_df = pd.DataFrame(records).drop_duplicates()
    print("UID instances:", uid_df.shape[0])
    return uid_df

# ====================
# PREVALENCE
# ====================

def compute_uid_prevalence(df: pd.DataFrame, uid_types: list[str], suffix: str):
    tmp = df.copy()
    tmp["has_uid"] = tmp["user_identifiers"].apply(
        lambda d: int(len(extract_uid_entries(d, uid_types)) > 0)
    )

    collapsed = (
        tmp.groupby(GROUP_COLS)["has_uid"]
        .max()
        .reset_index()
    )

    prevalence = (
        collapsed.groupby(["category", "location"])["has_uid"]
        .mean()
        .unstack()
        .multiply(100)
        .round(2)
    )

    prevalence.to_csv(f"{OUTPUT_DIR}/uid_prevalence_{suffix}.csv")

# ====================
# AVERAGE #UIDs PER ETLD
# ====================

def compute_avg_uids_per_etld(uid_df: pd.DataFrame, suffix: str):
    per_etld = (
        uid_df.groupby(GROUP_COLS)
        .size()
        .reset_index(name="num_uids")
    )

    avg = (
        per_etld.groupby(["category", "location"])["num_uids"]
        .mean()
        .round(2)
        .unstack()
    )

    avg.to_csv(f"{OUTPUT_DIR}/avg_uids_per_etld_{suffix}.csv")

# ====================
# THIRD-PARTY COUNTS
# ====================

def compute_avg_third_parties_per_etld(uid_df, mode, suffix):
    tmp = uid_df[uid_df["party"] == "third"].copy()

    tmp["tp_id"] = (
        tmp["caller_url"].apply(full_domain if mode == "full" else etld_plus_one)
    )
    tmp = tmp.dropna(subset=["tp_id"])

    per_etld = (
        tmp.groupby(GROUP_COLS)["tp_id"]
        .nunique()
        .reset_index(name="num_tps")
    )

    avg = (
        per_etld.groupby(["category", "location"])["num_tps"]
        .mean()
        .round(2)
        .unstack()
    )

    avg.to_csv(f"{OUTPUT_DIR}/avg_third_parties_per_etld_{suffix}_{mode}.csv")

def compute_total_third_parties(uid_df, mode, suffix):
    tmp = uid_df[uid_df["party"] == "third"].copy()
    tmp["tp_id"] = (
        tmp["caller_url"].apply(full_domain if mode == "full" else etld_plus_one)
    )

    result = (
        tmp.groupby(["category", "location"])["tp_id"]
        .nunique()
        .unstack(fill_value=0)
    )

    result.to_csv(f"{OUTPUT_DIR}/total_third_parties_{suffix}_{mode}.csv")

# ====================
# FP / TP DISTRIBUTION
# ====================

def compute_uid_fp_tp_distribution(uid_df: pd.DataFrame, suffix: str):
    inst = (
        uid_df.groupby(["category", "location", "party"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    inst["total"] = inst["first"] + inst["third"]
    inst["first_party_pct"] = (inst["first"] / inst["total"] * 100).round(2)
    inst["third_party_pct"] = (inst["third"] / inst["total"] * 100).round(2)

    inst.to_csv(
        f"{OUTPUT_DIR}/uid_fp_tp_distribution_{suffix}.csv",
        index=False
    )

# ====================
# TOP UID CREATORS
# ====================

def compute_top_uid_creators(uid_df, df, mode, suffix):
    tmp = uid_df[uid_df["party"] == "third"].copy()

    tmp["creator"] = (
        tmp["caller_url"].apply(full_domain if mode == "full" else etld_plus_one)
    )
    tmp = tmp.dropna(subset=["creator"])

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
        key = (r["category"], r["location"], r["etld"], r["creator"])
        if key not in seen:
            counts[(r["category"], r["location"], r["creator"])].add(r["etld"])
            seen.add(key)

    rows = []
    for (cat, loc, creator), etlds in counts.items():
        denom = denominators.get((cat, loc), 0)
        if denom == 0:
            continue
        pct = (len(etlds) / denom) * 100
        rows.append({
            "column": f"{cat}-{loc}",
            "creator": creator,
            "percentage": round(pct, 2),
        })

    out = pd.DataFrame(rows)
    result = {}

    for col, g in out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.creator} ({r.percentage}%)" for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()])) \
        .to_csv(f"{OUTPUT_DIR}/top_uid_creators_{suffix}_{mode}.csv", index=False)


def compute_top_uid_creators_country_level(uid_df, df, mode, suffix):
    """
    Country-level top third-party UID creators.
    - One column per country
    - Aggregates across all categories
    - Denominator = all etlds observed in that country
    """

    tmp = uid_df[uid_df["party"] == "third"].copy()

    # Choose identifier
    if mode == "full":
        tmp["creator"] = tmp["caller_url"].apply(full_domain)
    elif mode == "etld":
        tmp["creator"] = tmp["caller_url"].apply(etld_plus_one)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    tmp = tmp.dropna(subset=["creator"])

    # Denominator: all etlds per country
    denominators = (
        df[["location", "etld"]]
        .drop_duplicates()
        .groupby("location")
        .size()
        .to_dict()
    )

    seen = set()
    counts = defaultdict(set)

    # Deduplicate per (country, etld, creator)
    for _, r in tmp.iterrows():
        key = (r["location"], r["etld"], r["creator"])
        if key not in seen:
            counts[(r["location"], r["creator"])].add(r["etld"])
            seen.add(key)

    rows = []
    for (loc, creator), etlds in counts.items():
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue

        pct = (len(etlds) / denom) * 100
        rows.append({
            "column": loc,
            "creator": creator,
            "percentage": round(pct, 2),
        })

    out = pd.DataFrame(rows)

    result = {}
    for col, g in out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.creator} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/top_uid_creators_country_level_{suffix}_{mode}.csv",
            index=False
        )



def build_uid_key_instance_table(df: pd.DataFrame, uid_types: list[str]) -> pd.DataFrame:
    records = []

    for _, row in df.iterrows():
        page_etld = etld_plus_one(row["url"])
        uid_json = row["user_identifiers"]

        entries = extract_uid_entries(uid_json, uid_types)

        for e in entries:
            caller = e.get("caller_url")
            key = e.get("key")

            if not key or not is_valid_url(caller):
                continue

            caller_etld = etld_plus_one(caller)
            if caller_etld != page_etld:
                records.append({
                    "category": row["category"],
                    "location": row["location"],
                    "etld": row["etld"],
                    "caller_url": caller,
                    "storage_key": key,
                })

    return pd.DataFrame(records).drop_duplicates()

def compute_top_tp_key_pairs(uid_key_df, df, mode, suffix):
    """
    Top (third-party domain, storage key) pairs
    One column per (category, location)
    """

    tmp = uid_key_df.copy()

    if mode == "full":
        tmp["tp_id"] = tmp["caller_url"].apply(full_domain)
    elif mode == "etld":
        tmp["tp_id"] = tmp["caller_url"].apply(etld_plus_one)
    else:
        raise ValueError(mode)

    tmp = tmp.dropna(subset=["tp_id", "storage_key"])

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
        key = (r["category"], r["location"], r["etld"], r["tp_id"], r["storage_key"])
        if key not in seen:
            counts[(r["category"], r["location"], r["tp_id"], r["storage_key"])].add(r["etld"])
            seen.add(key)

    rows = []
    for (cat, loc, tp, sk), etlds in counts.items():
        denom = denominators.get((cat, loc), 0)
        if denom == 0:
            continue

        pct = (len(etlds) / denom) * 100
        rows.append({
            "column": f"{cat}-{loc}",
            "pair": f"{tp}::{sk}",
            "percentage": round(pct, 2),
        })

    out = pd.DataFrame(rows)

    result = {}
    for col, g in out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.pair} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/top_tp_key_pairs_{suffix}_{mode}.csv",
            index=False
        )


def compute_top_tp_key_pairs_country_level(uid_key_df, df, mode, suffix):
    """
    Country-level top (third-party domain, storage key) pairs
    Categories aggregated
    """

    tmp = uid_key_df.copy()

    if mode == "full":
        tmp["tp_id"] = tmp["caller_url"].apply(full_domain)
    elif mode == "etld":
        tmp["tp_id"] = tmp["caller_url"].apply(etld_plus_one)
    else:
        raise ValueError(mode)

    tmp = tmp.dropna(subset=["tp_id", "storage_key"])

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
        key = (r["location"], r["etld"], r["tp_id"], r["storage_key"])
        if key not in seen:
            counts[(r["location"], r["tp_id"], r["storage_key"])].add(r["etld"])
            seen.add(key)

    rows = []
    for (loc, tp, sk), etlds in counts.items():
        denom = denominators.get(loc, 0)
        if denom == 0:
            continue

        pct = (len(etlds) / denom) * 100
        rows.append({
            "column": loc,
            "pair": f"{tp}::{sk}",
            "percentage": round(pct, 2),
        })

    out = pd.DataFrame(rows)

    result = {}
    for col, g in out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.pair} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/top_tp_key_pairs_country_level_{suffix}_{mode}.csv",
            index=False
        )




def generate_token_variants(token: str) -> set[str]:
    variants = set()
    if not token or not isinstance(token, str):
        return variants

    try:
        raw = token.encode("utf-8")
        variants.add(token)

        md5 = hashlib.md5(raw).hexdigest()
        sha1 = hashlib.sha1(raw).hexdigest()
        sha256 = hashlib.sha256(raw).hexdigest()
        b64 = base64.b64encode(raw).decode("utf-8")

        variants.update([md5, sha1, sha256, b64])

        variants.add(hashlib.md5(md5.encode()).hexdigest())
        variants.add(hashlib.sha1(sha1.encode()).hexdigest())
        variants.add(hashlib.sha256(sha256.encode()).hexdigest())

    except Exception:
        pass

    return variants


def build_uid_exfiltration_table(df, uid_types):
    """
    Optimized detection of UID exfiltration via network requests.
    Semantics identical to original version.
    """

    start = time.time()
    print("[*] Building UID exfiltration table")

    load_dotenv()
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}/"
        f"{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )

    # ------------------------------------------------------------
    # Load requests ONCE
    # ------------------------------------------------------------
    reqs = pd.read_sql("""
        SELECT
            r.session_id,
            r.request_url,
            r.result_headers,
            r.security_headers,
            r.redirects,
            s.etld,
            s.category,
            s.location,
            s.url AS page_url
        FROM requests r
        JOIN crawl_sessions s ON r.session_id = s.id
    """, engine)

    # ------------------------------------------------------------
    # Pre-build haystacks (CRITICAL OPTIMIZATION)
    # ------------------------------------------------------------
    reqs["haystack"] = (
        reqs["request_url"].astype(str) + " " +
        reqs["result_headers"].astype(str) + " " +
        reqs["security_headers"].astype(str) + " " +
        reqs["redirects"].astype(str)
    )

    # ------------------------------------------------------------
    # Group requests by session_id (CRITICAL OPTIMIZATION)
    # ------------------------------------------------------------
    reqs_by_session = {
        sid: g
        for sid, g in reqs.groupby("session_id")
    }

    records = []
    seen = set()

    # ------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------
    for _, row in tqdm(df.iterrows(), total=len(df), desc="UID exfiltration"):

        page_etld = etld_plus_one(row["url"])
        uid_entries = extract_uid_entries(row["user_identifiers"], uid_types)

        if not uid_entries:
            continue

        sess_reqs = reqs_by_session.get(row["id"])
        if sess_reqs is None or sess_reqs.empty:
            continue

        for e in uid_entries:
            token = e.get("val")
            storage_key = e.get("key")
            setter_url = e.get("caller_url")

            if not token or not storage_key or not is_valid_url(setter_url):
                continue

            setter_domain = etld_plus_one(setter_url)
            if not setter_domain:
                continue

            # ----------------------------------------------------
            # Generate token variants ONCE per UID
            # ----------------------------------------------------
            variants = generate_token_variants(token)
            if not variants:
                continue

            # ----------------------------------------------------
            # Scan requests
            # ----------------------------------------------------
            for _, req in sess_reqs.iterrows():

                receiver_domain = etld_plus_one(req["request_url"])

                # must be third-party AND different from setter AND page
                if (
                    not receiver_domain or
                    receiver_domain == setter_domain or
                    receiver_domain == page_etld
                ):
                    continue

                haystack = req["haystack"]

                # substring match
                if any(v in haystack for v in variants):
                    key = (
                        row["etld"],
                        setter_domain,
                        storage_key,
                        receiver_domain
                    )

                    if key not in seen:
                        records.append({
                            "category": row["category"],
                            "location": row["location"],
                            "etld": row["etld"],
                            "setter_domain": setter_domain,
                            "storage_key": storage_key,
                            "receiver_domain": receiver_domain,
                        })
                        seen.add(key)

    exf_df = pd.DataFrame(records)
    print(f"[+] UID exfiltration events: {len(exf_df)}")
    print(f"[+] Time elapsed: {time.time() - start:.1f}s")

    return exf_df

def compute_uid_exfiltration_prevalence(exf_df, df, suffix):
    all_etlds = (
        df[["category", "location", "etld"]]
        .drop_duplicates()
    )

    exf_presence = (
        exf_df.groupby(["category", "location", "etld"])
        .size()
        .reset_index(name="has_exf")
    )

    merged = all_etlds.merge(
        exf_presence,
        on=["category", "location", "etld"],
        how="left"
    ).fillna(0)

    prevalence = (
        merged.groupby(["category", "location"])["has_exf"]
        .apply(lambda x: (x > 0).mean())
        .unstack()
        .multiply(100)
        .round(2)
    )

    prevalence.to_csv(
        f"{OUTPUT_DIR}/uid_exfiltration_prevalence_{suffix}.csv"
    )


def compute_uid_exfiltration_prevalence_country(exf_df, df, suffix):
    all_etlds = (
        df[["location", "etld"]]
        .drop_duplicates()
    )

    exf_presence = (
        exf_df.groupby(["location", "etld"])
        .size()
        .reset_index(name="has_exf")
    )

    merged = all_etlds.merge(
        exf_presence,
        on=["location", "etld"],
        how="left"
    ).fillna(0)

    prevalence = (
        merged.groupby("location")["has_exf"]
        .apply(lambda x: (x > 0).mean() * 100)
        .round(2)
    )

    prevalence.to_csv(
        f"{OUTPUT_DIR}/uid_exfiltration_prevalence_country_{suffix}.csv"
    )

def compute_top_exfiltrated_identifiers(exf_df, df, suffix):
    denominators = (
        df[["category", "location", "etld"]]
        .drop_duplicates()
        .groupby(["category", "location"])
        .size()
        .to_dict()
    )

    counts = defaultdict(set)

    for _, r in exf_df.iterrows():
        counts[
            (r["category"], r["location"],
             r["setter_domain"], r["storage_key"])
        ].add(r["etld"])

    rows = []
    for (cat, loc, dom, key), etlds in counts.items():
        pct = len(etlds) / denominators[(cat, loc)] * 100
        rows.append({
            "column": f"{cat}-{loc}",
            "identifier": f"{dom}::{key}",
            "percentage": round(pct, 2),
        })

    out = pd.DataFrame(rows)
    result = {}

    for col, g in out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.identifier} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/top_exfiltrated_identifiers_{suffix}.csv",
            index=False
        )


def compute_top_exfiltration_receivers(exf_df, df, suffix):
    denominators = (
        df[["category", "location", "etld"]]
        .drop_duplicates()
        .groupby(["category", "location"])
        .size()
        .to_dict()
    )

    counts = defaultdict(set)

    for _, r in exf_df.iterrows():
        counts[
            (r["category"], r["location"], r["receiver_domain"])
        ].add(r["etld"])

    rows = []
    for (cat, loc, dom), etlds in counts.items():
        pct = len(etlds) / denominators[(cat, loc)] * 100
        rows.append({
            "column": f"{cat}-{loc}",
            "receiver": dom,
            "percentage": round(pct, 2),
        })

    out = pd.DataFrame(rows)
    result = {}

    for col, g in out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.receiver} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/top_exfiltration_receivers_{suffix}.csv",
            index=False
        )

def compute_top_exfiltration_instances(exf_df, df, suffix):
    denominators = (
        df[["category", "location", "etld"]]
        .drop_duplicates()
        .groupby(["category", "location"])
        .size()
        .to_dict()
    )

    counts = defaultdict(set)

    for _, r in exf_df.iterrows():
        counts[
            (r["category"], r["location"],
             r["setter_domain"], r["storage_key"], r["receiver_domain"])
        ].add(r["etld"])

    rows = []
    for (cat, loc, sdom, key, rdom), etlds in counts.items():
        pct = len(etlds) / denominators[(cat, loc)] * 100
        rows.append({
            "column": f"{cat}-{loc}",
            "instance": f"{sdom}::{key}→{rdom}",
            "percentage": round(pct, 2),
        })

    out = pd.DataFrame(rows)
    result = {}

    for col, g in out.groupby("column"):
        g = g.sort_values("percentage", ascending=False).head(TOP_K)
        result[col] = [
            f"{r.instance} ({r.percentage}%)"
            for r in g.itertuples()
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in result.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/top_exfiltration_instances_{suffix}.csv",
            index=False
        )

# ====================
# MAIN
# ====================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = load_data()

    for suffix, uid_types in UID_CONFIGS.items():
        print(f"=== UID ANALYSIS: {suffix} ===")

        compute_uid_prevalence(df, uid_types, suffix)

        uid_df = build_uid_instance_table(df, uid_types)

        compute_avg_uids_per_etld(uid_df, suffix)
        compute_uid_fp_tp_distribution(uid_df, suffix)

        for mode in ["full", "etld"]:
            compute_avg_third_parties_per_etld(uid_df, mode, suffix)
            compute_total_third_parties(uid_df, mode, suffix)
            compute_top_uid_creators(uid_df, df, mode, suffix)
            compute_top_uid_creators_country_level(uid_df, df, mode, suffix)



        uid_key_df = build_uid_key_instance_table(df, uid_types)


        for mode in ["full", "etld"]:
            compute_top_tp_key_pairs(uid_key_df, df, mode, suffix)
            compute_top_tp_key_pairs_country_level(uid_key_df, df, mode, suffix)


        exf_df = build_uid_exfiltration_table(df, uid_types)

        compute_uid_exfiltration_prevalence(exf_df, df, suffix)
        compute_uid_exfiltration_prevalence_country(exf_df, df, suffix)

        compute_top_exfiltrated_identifiers(exf_df, df, suffix)
        compute_top_exfiltration_receivers(exf_df, df, suffix)
        compute_top_exfiltration_instances(exf_df, df, suffix)

if __name__ == "__main__":
    main()



