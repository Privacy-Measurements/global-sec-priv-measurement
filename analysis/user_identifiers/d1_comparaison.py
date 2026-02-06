import os
import json
import random
from urllib.parse import urlparse
from collections import defaultdict
from tqdm import tqdm

import pandas as pd
import tldextract
from dotenv import load_dotenv
from sqlalchemy import create_engine


# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = "reports_D1_user_identifiers"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# HELPERS
# ============================================================

def is_valid_url(u: str) -> bool:
    try:
        return urlparse(u).scheme in {"http", "https"}
    except Exception:
        return False


def normalize_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}"


def etld_plus_one(url: str) -> str | None:
    if not is_valid_url(url):
        return None
    ext = tldextract.extract(url)
    return ext.top_domain_under_public_suffix


def is_first_party(caller_url: str, page_url: str) -> bool:
    if not is_valid_url(caller_url):
        return True
    return etld_plus_one(caller_url) == etld_plus_one(page_url)


def full_domain(url: str) -> str | None:
    try:
        return urlparse(url).hostname
    except Exception:
        return None


# ============================================================
# DATA LOADING
# ============================================================

def load_tables():
    load_dotenv()
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}/"
        f"{os.getenv('DB_NAME')}"
    )

    sessions = pd.read_sql("""
        SELECT
            id AS session_id,
            etld,
            url,
            location,
            user_identifiers
        FROM crawl_sessions
        WHERE category = 'global'
          AND location NOT LIKE '%%VPN'
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
    """, engine)

    sessions["user_identifiers"] = sessions["user_identifiers"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else {}
    )
    sessions["normalized_url"] = sessions["url"].apply(normalize_url)

    return sessions


# ============================================================
# UNIVERSE
# ============================================================

def build_common_universe(sessions: pd.DataFrame):
    loc_count = sessions["location"].nunique()
    counts = (
        sessions.groupby(["etld", "normalized_url"])["location"]
        .nunique()
    )
    return set(counts[counts == loc_count].index.tolist())


# ============================================================
# IDENTIFIER EXTRACTION
# ============================================================

def extract_identifiers(session_row):
    """
    Returns list of (caller_url, party)
    """
    out = []

    for key in ["cookies_storage_identifiers", "local_storage_identifiers"]:
        for e in session_row.user_identifiers.get(key, []):
            caller = e.get("caller_url")
            if not caller:
                continue

            party = (
                "first"
                if is_first_party(caller, session_row.url)
                else "third"
            )

            out.append((caller, party))

    return out


# ============================================================
# D1 PAIRWISE ANALYSIS
# ============================================================

def analyze_user_identifiers_D1(sessions, universe):

    locations = sorted(sessions["location"].unique())

    # fast lookup
    session_index = {
        (r.location, r.etld, r.normalized_url): r
        for r in sessions.itertuples(index=False)
    }

    # identifiers per session
    identifiers = {
        r.session_id: extract_identifiers(r)
        for r in sessions.itertuples(index=False)
    }

    # stats
    pairwise_rows = []
    examples = defaultdict(list)

    overall = defaultdict(int)

    # NEW: third-party causers
    tp_causers_per_L1 = {
        L1: defaultdict(set) for L1 in locations
    }
    tp_causers_overall = defaultdict(set)

    for L1 in tqdm(locations, desc="L1 locations"):
        for L2 in locations:
            if L1 == L2:
                continue

            total = fp = tp = 0

            for (etld, norm_url) in universe:
                s1 = session_index.get((L1, etld, norm_url))
                s2 = session_index.get((L2, etld, norm_url))
                if not s1 or not s2:
                    continue

                ids_L1 = identifiers[s1.session_id]
                urls_L2 = {u for u, _ in identifiers[s2.session_id]}

                seen = set()

                for caller_url, party in ids_L1:
                    if caller_url in seen:
                        continue
                    seen.add(caller_url)

                    if caller_url in urls_L2:
                        continue

                    total += 1
                    overall["total"] += 1

                    if party == "first":
                        fp += 1
                        overall["fp"] += 1
                        cls = "first_party"
                    else:
                        tp += 1
                        overall["tp"] += 1
                        cls = "third_party"

                        dom = full_domain(caller_url)
                        if dom:
                            tp_causers_per_L1[L1][dom].add(etld)
                            tp_causers_overall[dom].add(etld)

                    examples[L1].append({
                        "L1": L1,
                        "L2": L2,
                        "etld": etld,
                        "normalized_url": norm_url,
                        "caller_url": caller_url,
                        "party": cls,
                    })

            if total > 0:
                pairwise_rows.append({
                    "from": L1,
                    "to": L2,
                    "total_identifiers": total,
                    "first_party_pct": round(fp / total * 100, 2),
                    "third_party_pct": round(tp / total * 100, 2),
                })

    # pairwise
    pd.DataFrame(pairwise_rows).to_csv(
        f"{OUTPUT_DIR}/pairwise_user_identifiers_D1.csv",
        index=False,
    )

    # overall
    total = overall["total"]
    pd.DataFrame([{
        "total_identifiers": total,
        "first_party_pct": round(overall["fp"] / total * 100, 2),
        "third_party_pct": round(overall["tp"] / total * 100, 2),
    }]).to_csv(
        f"{OUTPUT_DIR}/pairwise_user_identifiers_D1_overall.csv",
        index=False,
    )

    return tp_causers_per_L1, tp_causers_overall, examples


# ============================================================
# TOP THIRD-PARTY CAUSERS
# ============================================================

def write_top_third_party_identifier_causers(
    tp_causers_per_L1,
    tp_causers_overall,
    sessions,
    TOP_K=10,
):
    denom_per_country = (
        sessions[["location", "etld"]]
        .drop_duplicates()
        .groupby("location")
        .size()
        .to_dict()
    )

    # per location
    table = {}
    for L1, dom_map in tp_causers_per_L1.items():
        denom = denom_per_country.get(L1, 0)
        ranked = sorted(
            dom_map.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )[:TOP_K]

        table[L1] = [
            f"{dom} ({len(etlds) / denom * 100:.2f}%)"
            for dom, etlds in ranked
            if denom > 0
        ]

    pd.DataFrame(dict([(k, pd.Series(v)) for k, v in table.items()])) \
        .to_csv(
            f"{OUTPUT_DIR}/D1_top_third_party_identifier_causers_L1.csv",
            index=False,
        )

    # overall
    total_etlds = sessions["etld"].nunique()
    ranked_overall = sorted(
        tp_causers_overall.items(),
        key=lambda x: len(x[1]),
        reverse=True
    )[:TOP_K]

    pd.DataFrame({
        "overall": [
            f"{dom} ({len(etlds) / total_etlds * 100:.2f}%)"
            for dom, etlds in ranked_overall
            if total_etlds > 0
        ]
    }).to_csv(
        f"{OUTPUT_DIR}/D1_top_third_party_identifier_causers_overall.csv",
        index=False,
    )


# ============================================================
# EXAMPLES
# ============================================================

def write_examples(examples, k=5):
    for L1, rows in examples.items():
        if rows:
            pd.DataFrame(random.sample(rows, min(k, len(rows)))).to_csv(
                f"{OUTPUT_DIR}/D1_user_identifier_examples_{L1}.csv",
                index=False,
            )


# ============================================================
# MAIN
# ============================================================

def main():
    sessions = load_tables()
    universe = build_common_universe(sessions)

    tp_causers_per_L1, tp_causers_overall, examples = analyze_user_identifiers_D1(
        sessions, universe
    )

    write_top_third_party_identifier_causers(
        tp_causers_per_L1,
        tp_causers_overall,
        sessions,
    )

    write_examples(examples)


if __name__ == "__main__":
    main()
