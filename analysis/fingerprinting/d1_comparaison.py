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

OUTPUT_DIR = "reports_D1"
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
            category,
            location,
            fingerprinting
        FROM crawl_sessions
        WHERE category = 'global'
          AND location NOT LIKE '%%VPN'
          AND (etld_url_rel IS NULL OR etld_url_rel <> 'cross-site')
          AND (is_cloudflare IS NULL OR is_cloudflare = 0)
    """, engine)

    html_elements = pd.read_sql("""
        SELECT
            session_id,
            element_id,
            parent_id,
            src
        FROM html_elements
    """, engine)

    scripts = pd.read_sql("""
        SELECT
            session_id,
            script_id,
            executor_id,
            script_src,
            script_hash
        FROM scripts
    """, engine)

    sessions["fingerprinting"] = sessions["fingerprinting"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else {}
    )
    sessions["normalized_url"] = sessions["url"].apply(normalize_url)

    return sessions, html_elements, scripts


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
# INCLUSION GRAPH
# ============================================================

def build_node_caches(html_elements: pd.DataFrame, scripts: pd.DataFrame):
    html_cache = {
        (r.session_id, r.element_id): (r.parent_id, r.src)
        for r in html_elements.itertuples(index=False)
    }

    script_cache = {
        (r.session_id, r.script_id): (r.executor_id, r.script_src)
        for r in scripts.itertuples(index=False)
    }

    return html_cache, script_cache


def build_inclusion_chain(session_id, caller_id, html_cache, script_cache):
    chain = []
    cur = caller_id

    while True:
        key = (session_id, cur)

        if key in html_cache:
            parent, src = html_cache[key]
        elif key in script_cache:
            parent, src = script_cache[key]
        else:
            break

        if src:
            chain.append(src)

        if not parent:
            break

        cur = parent

    return chain[::-1]


def match_chain(session_id, chain, session_srcs):
    last_match = None
    srcs = session_srcs.get(session_id, set())

    for src in chain:
        if src not in srcs:
            break
        last_match = src

    return last_match


# ============================================================
# PAIRWISE + L1-CENTRIC ANALYSIS
# ============================================================

def analyze_pairwise(sessions, html_elements, scripts, universe):

    locations = sorted(sessions["location"].unique())

    # ------------------------------------------------------------
    # Fast session lookup
    # ------------------------------------------------------------
    session_index = {
        (r.location, r.etld, r.normalized_url): r
        for r in sessions.itertuples(index=False)
    }

    # ------------------------------------------------------------
    # Extract fingerprinting callers per session
    # ------------------------------------------------------------
    fp_callers = {}
    for r in sessions.itertuples(index=False):
        lst = []
        for v in r.fingerprinting.values():
            for e in v:
                cid = e.get("caller_id") or e.get("script_id")
                curl = e.get("caller_url") or e.get("script_src")
                if cid and curl:
                    lst.append((cid, curl))
        fp_callers[r.session_id] = lst

    # ------------------------------------------------------------
    # Inclusion graph caches
    # ------------------------------------------------------------
    html_cache, script_cache = build_node_caches(html_elements, scripts)

    session_srcs = defaultdict(set)
    for (sid, _), (_, src) in html_cache.items():
        if src:
            session_srcs[sid].add(src)
    for (sid, _), (_, src) in script_cache.items():
        if src:
            session_srcs[sid].add(src)

    # ------------------------------------------------------------
    # Script hash lookup
    # ------------------------------------------------------------
    script_hash_map = {
        (r.session_id, r.script_src): r.script_hash
        for r in scripts.itertuples(index=False)
        if r.script_src and r.script_hash
    }

    # ------------------------------------------------------------
    # Inclusion-chain memoization
    # ------------------------------------------------------------
    chain_cache = {}
    def get_chain(session_id, caller_id):
        key = (session_id, caller_id)
        if key not in chain_cache:
            chain_cache[key] = build_inclusion_chain(
                session_id, caller_id, html_cache, script_cache
            )
        return chain_cache[key]

    # ------------------------------------------------------------
    # Stats containers
    # ------------------------------------------------------------
    exclusive_stats = {
        L1: {
            "fp_no_match": defaultdict(int),
            "fp_same_hash": defaultdict(int),
            "fp_diff_hash": defaultdict(int),
            "tp_same_hash_etld": defaultdict(int),
            "tp_same_hash_full": defaultdict(int),
            "tp_diff_hash_etld": defaultdict(int),
            "tp_diff_hash_full": defaultdict(int),
            "total_fp": 0,
            "total_tp": 0,
        }
        for L1 in locations
    }

    examples = {L1: defaultdict(list) for L1 in locations}
    pairwise_rows = []

    # ------------------------------------------------------------
    # OVERALL counters (NEW)
    # ------------------------------------------------------------
    overall = defaultdict(int)

    # ============================================================
    # Pairwise analysis
    # ============================================================
    for L1 in tqdm(locations, desc="L1 locations"):
        for L2 in locations:
            if L1 == L2:
                continue

            total = 0

            fp = fp_no = fp_same = fp_diff = 0
            tp = tp_same = tp_diff = 0

            for (etld, norm_url) in universe:
                s1 = session_index.get((L1, etld, norm_url))
                s2 = session_index.get((L2, etld, norm_url))
                if not s1 or not s2:
                    continue

                callers_L1 = fp_callers[s1.session_id]
                urls_L2 = {u for _, u in fp_callers[s2.session_id]}

                seen_ids = set()

                for caller_id, caller_url in callers_L1:
                    if caller_id in seen_ids:
                        continue
                    seen_ids.add(caller_id)

                    if caller_url in urls_L2:
                        continue

                    total += 1
                    overall["total"] += 1

                    chain = get_chain(s1.session_id, caller_id)
                    match = match_chain(s2.session_id, chain, session_srcs)

                    if not match:
                        cls = "fp_no_match"
                        fp += 1
                        fp_no += 1
                        overall["fp"] += 1
                        overall["fp_no_match"] += 1
                        exclusive_stats[L1]["fp_no_match"][urlparse(caller_url).path] += 1
                        exclusive_stats[L1]["total_fp"] += 1
                        hash_L1 = script_hash_map.get((s1.session_id, caller_url))
                        hash_L2 = None

                    else:
                        hash_L1 = script_hash_map.get((s1.session_id, match))
                        hash_L2 = script_hash_map.get((s2.session_id, match))
                        same_hash = hash_L1 and hash_L2 and hash_L1 == hash_L2

                        if is_first_party(match, s2.url):
                            fp += 1
                            overall["fp"] += 1
                            exclusive_stats[L1]["total_fp"] += 1

                            if same_hash:
                                cls = "fp_same_hash"
                                fp_same += 1
                                overall["fp_same_hash"] += 1
                                exclusive_stats[L1]["fp_same_hash"][urlparse(caller_url).path] += 1
                            else:
                                cls = "fp_diff_hash"
                                fp_diff += 1
                                overall["fp_diff_hash"] += 1
                                exclusive_stats[L1]["fp_diff_hash"][urlparse(caller_url).path] += 1
                        else:
                            tp += 1
                            overall["tp"] += 1
                            exclusive_stats[L1]["total_tp"] += 1

                            if same_hash:
                                cls = "tp_same_hash"
                                tp_same += 1
                                overall["tp_same_hash"] += 1
                                exclusive_stats[L1]["tp_same_hash_etld"][etld_plus_one(caller_url)] += 1
                                exclusive_stats[L1]["tp_same_hash_full"][urlparse(caller_url).hostname] += 1
                            else:
                                cls = "tp_diff_hash"
                                tp_diff += 1
                                overall["tp_diff_hash"] += 1
                                exclusive_stats[L1]["tp_diff_hash_etld"][etld_plus_one(caller_url)] += 1
                                exclusive_stats[L1]["tp_diff_hash_full"][urlparse(caller_url).hostname] += 1

                    examples[L1][cls].append({
                        "L1": L1,
                        "L2": L2,
                        "session_L1": s1.session_id,
                        "session_L2": s2.session_id,
                        "etld": etld,
                        "normalized_url": norm_url,
                        "caller_url": caller_url,
                        "matched_src": match,
                        "hash_L1": hash_L1,
                        "hash_L2": hash_L2,
                        "chain_L1": " -> ".join(chain),
                        "classification": cls,
                    })

            if total > 0:
                pairwise_rows.append({
                    "from": L1,
                    "to": L2,
                    "total_callers": total,

                    "first_party_pct": round(fp / total * 100, 2),
                    "first_party_no_match_pct": round(fp_no / total * 100, 2),
                    "first_party_same_hash_pct": round(fp_same / total * 100, 2),
                    "first_party_diff_hash_pct": round(fp_diff / total * 100, 2),

                    "third_party_pct": round(tp / total * 100, 2),
                    "third_party_same_hash_pct": round(tp_same / total * 100, 2),
                    "third_party_diff_hash_pct": round(tp_diff / total * 100, 2),
                })

    # ------------------------------------------------------------
    # Write pairwise CSV
    # ------------------------------------------------------------
    pd.DataFrame(pairwise_rows).to_csv(
        f"{OUTPUT_DIR}/pairwise_attribution_D1.csv",
        index=False
    )

    # ------------------------------------------------------------
    # Write OVERALL CSV (NEW)
    # ------------------------------------------------------------
    total = overall["total"]
    overall_row = {
        "total_callers": total,
        "first_party_pct": round(overall["fp"] / total * 100, 2),
        "first_party_no_match_pct": round(overall["fp_no_match"] / total * 100, 2),
        "first_party_same_hash_pct": round(overall["fp_same_hash"] / total * 100, 2),
        "first_party_diff_hash_pct": round(overall["fp_diff_hash"] / total * 100, 2),
        "third_party_pct": round(overall["tp"] / total * 100, 2),
        "third_party_same_hash_pct": round(overall["tp_same_hash"] / total * 100, 2),
        "third_party_diff_hash_pct": round(overall["tp_diff_hash"] / total * 100, 2),
    }

    pd.DataFrame([overall_row]).to_csv(
        f"{OUTPUT_DIR}/pairwise_attribution_D1_overall.csv",
        index=False
    )

    return exclusive_stats, examples


# ============================================================
# OUTPUT HELPERS
# ============================================================

def write_top_tables(exclusive_stats):

    # ------------------------------------------------------------
    # Helper: build per-location tables
    # ------------------------------------------------------------
    def per_L1(keys, total_key):
        table = {}
        for L1, stats in exclusive_stats.items():
            total = stats[total_key]
            merged = defaultdict(int)

            for k in keys:
                for item, cnt in stats[k].items():
                    merged[item] += cnt

            ranked = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:10]
            table[L1] = [
                f"{item} ({(cnt / total * 100):.2f}%)"
                for item, cnt in ranked
                if total > 0
            ]

        return pd.DataFrame(dict([(k, pd.Series(v)) for k, v in table.items()]))

    # ------------------------------------------------------------
    # Helper: build overall tables
    # ------------------------------------------------------------
    def overall(keys, total_key):
        merged = defaultdict(int)
        total = 0

        for stats in exclusive_stats.values():
            total += stats[total_key]
            for k in keys:
                for item, cnt in stats[k].items():
                    merged[item] += cnt

        ranked = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:10]
        return pd.DataFrame({
            "overall": [
                f"{item} ({(cnt / total * 100):.2f}%)"
                for item, cnt in ranked
                if total > 0
            ]
        })

    # ============================================================
    # FIRST-PARTY TABLES (script path)
    # ============================================================

    per_L1(
        keys=["fp_no_match"],
        total_key="total_fp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_fp_no_match_L1.csv", index=False)

    per_L1(
        keys=["fp_same_hash"],
        total_key="total_fp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_fp_same_hash_L1.csv", index=False)

    per_L1(
        keys=["fp_diff_hash"],
        total_key="total_fp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_fp_diff_hash_L1.csv", index=False)

    overall(
        keys=["fp_no_match"],
        total_key="total_fp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_fp_no_match_overall.csv", index=False)

    overall(
        keys=["fp_same_hash"],
        total_key="total_fp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_fp_same_hash_overall.csv", index=False)

    overall(
        keys=["fp_diff_hash"],
        total_key="total_fp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_fp_diff_hash_overall.csv", index=False)

    # ============================================================
    # THIRD-PARTY TABLES (eTLD+1)
    # ============================================================

    per_L1(
        keys=["tp_same_hash_etld"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_same_hash_etld_L1.csv", index=False)

    per_L1(
        keys=["tp_diff_hash_etld"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_diff_hash_etld_L1.csv", index=False)

    overall(
        keys=["tp_same_hash_etld"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_same_hash_etld_overall.csv", index=False)

    overall(
        keys=["tp_diff_hash_etld"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_diff_hash_etld_overall.csv", index=False)

    # ============================================================
    # THIRD-PARTY TABLES (full hostname)
    # ============================================================

    per_L1(
        keys=["tp_same_hash_full"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_same_hash_full_L1.csv", index=False)

    per_L1(
        keys=["tp_diff_hash_full"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_diff_hash_full_L1.csv", index=False)

    overall(
        keys=["tp_same_hash_full"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_same_hash_full_overall.csv", index=False)

    overall(
        keys=["tp_diff_hash_full"],
        total_key="total_tp"
    ).to_csv(f"{OUTPUT_DIR}/D1_top_tp_diff_hash_full_overall.csv", index=False)

    

def write_examples(examples, k=5):
    for L1, groups in examples.items():
        rows = []
        for cls, lst in groups.items():
            for ex in random.sample(lst, min(k, len(lst))):
                ex["example_type"] = cls
                rows.append(ex)

        if rows:
            pd.DataFrame(rows).to_csv(
                f"{OUTPUT_DIR}/D1_examples_{L1}.csv",
                index=False
            )


# ============================================================
# MAIN
# ============================================================

def main():
    sessions, html_elements, scripts = load_tables()
    universe = build_common_universe(sessions)

    print(f"[+] Common pages: {len(universe)}")

    exclusive_stats, examples = analyze_pairwise(
        sessions, html_elements, scripts, universe
    )

    write_top_tables(exclusive_stats)
    write_examples(examples)


if __name__ == "__main__":
    main()
