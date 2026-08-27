import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from analysis.utils.urls import extract_domain_from_url, is_http_url, normalize_domain


def _normalize_node_id(value):
    # Graph node ids are stored as strings like n12345.
    if value is None or pd.isna(value):
        return None

    raw = str(value).strip()
    if not raw:
        return None
    return raw


def _safe_src_from_attrs(attrs_json_value):
    # Extract src from attrs_json whether it is already a dict or a JSON string.
    if attrs_json_value is None or pd.isna(attrs_json_value):
        return ""

    if isinstance(attrs_json_value, dict):
        return str(attrs_json_value.get("src", "") or "")

    try:
        parsed = json.loads(attrs_json_value)
        if isinstance(parsed, dict):
            return str(parsed.get("src", "") or "")
    except Exception:
        return ""

    return ""


def _classify_reason(n_src_nodes_in_chain, last_matching_src, observed_etld):
    # Attribute the difference to first_party/third_party using the same rules as D1.
    if n_src_nodes_in_chain == 0:
        return "first_party"

    if not last_matching_src:
        return "first_party"

    if not is_http_url(last_matching_src):
        return "first_party"

    src_domain = normalize_domain(extract_domain_from_url(last_matching_src))
    etld_domain = normalize_domain(observed_etld)
    if not src_domain or not etld_domain:
        return "first_party"

    if src_domain == etld_domain:
        return "first_party"

    return "third_party"


def _load_dom_context_for_sessions(session_ids):
    # Load only scripts/html elements required by the selected representative sessions.
    if not session_ids:
        return pd.DataFrame(), pd.DataFrame()

    load_dotenv()
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}/"
        f"{os.getenv('DB_NAME')}",
        pool_recycle=3600,
    )

    id_list_sql = ",".join(str(int(x)) for x in sorted(session_ids))

    scripts = pd.read_sql(
        f"""
        SELECT
            session_id,
            script_id,
            parent_id,
            script_src
        FROM scripts
        WHERE session_id IN ({id_list_sql})
        """,
        engine,
    )

    html_elements = pd.read_sql(
        f"""
        SELECT
            session_id,
            element_id,
            parent_id,
            attrs_json
        FROM html_elements
        WHERE session_id IN ({id_list_sql})
        """,
        engine,
    )

    return scripts, html_elements


def _build_parent_indexes(scripts_df, html_df):
    # Build O(1) node lookups plus a per-session inventory of src values.
    scripts_index = {}
    html_index = {}
    session_src_values = {}

    for row in scripts_df.itertuples(index=False):
        session_id = int(row.session_id)
        script_id = _normalize_node_id(row.script_id)
        parent_id = _normalize_node_id(row.parent_id)
        if not script_id:
            continue

        src = "" if pd.isna(row.script_src) else str(row.script_src)
        scripts_index[(session_id, script_id)] = {
            "parent_id": parent_id,
            "src": src,
            "node_type": "script",
        }
        if src:
            session_src_values.setdefault(session_id, set()).add(src)

    for row in html_df.itertuples(index=False):
        session_id = int(row.session_id)
        element_id = _normalize_node_id(row.element_id)
        parent_id = _normalize_node_id(row.parent_id)
        if not element_id:
            continue

        src = _safe_src_from_attrs(row.attrs_json)
        html_index[(session_id, element_id)] = {
            "parent_id": parent_id,
            "src": src,
            "node_type": "html_element",
        }
        if src:
            session_src_values.setdefault(session_id, set()).add(src)

    return scripts_index, html_index, session_src_values


def _trace_inclusion_chain(session_id, start_parent_id, scripts_index, html_index):
    # Follow request.parent_id upward until the chain ends or becomes unresolved.
    chain = []
    current_id = _normalize_node_id(start_parent_id)
    if not current_id:
        return chain

    seen = set()
    depth = 0

    while current_id not in seen:
        seen.add(current_id)

        key = (int(session_id), current_id)
        node = scripts_index.get(key)
        if node is None:
            node = html_index.get(key)

        if node is None:
            chain.append(
                {
                    "depth": depth,
                    "node_id": current_id,
                    "node_type": "not_found",
                    "src": "",
                    "parent_id": None,
                }
            )
            break

        chain.append(
            {
                "depth": depth,
                "node_id": current_id,
                "node_type": node["node_type"],
                "src": node["src"],
                "parent_id": node["parent_id"],
            }
        )

        if node["parent_id"] is None:
            break

        current_id = _normalize_node_id(node["parent_id"])
        if not current_id:
            break
        depth += 1

    return chain


def _select_session_level_cases(pair_universe, paired_sessions, all_requests, tracker_requests, pair_tracker_sets):
    # Build one representative case per (pair, side, tracker) difference.
    print("[d2-chain] selecting representative cases...", flush=True)
    req_counts = all_requests.groupby("session_id").size().to_dict()

    session_meta = paired_sessions[
        ["session_id", "pair_id", "country", "site_side", "d2_etld", "equivalent_etld"]
    ].drop_duplicates().copy()
    session_meta["request_count"] = session_meta["session_id"].map(lambda sid: req_counts.get(sid, 0))

    best_dest_df = session_meta.sort_values(
        ["request_count", "session_id"],
        ascending=[False, True],
    ).drop_duplicates(["pair_id", "site_side"])
    best_dest_map = {
        (row.pair_id, row.site_side): {
            "session_id": int(row.session_id),
            "request_count": int(row.request_count),
            "observed_etld": row.d2_etld if row.site_side == "D2" else row.equivalent_etld,
        }
        for row in best_dest_df.itertuples(index=False)
    }

    tracker_df = tracker_requests.merge(
        session_meta[["session_id", "pair_id", "site_side", "request_count"]],
        on="session_id",
        how="inner",
    )
    tracker_df["tracker_domain"] = tracker_df["request_url"].apply(extract_domain_from_url)
    tracker_df = tracker_df.dropna(subset=["tracker_domain"])

    best_obs_df = tracker_df.sort_values(
        ["request_count", "session_id", "request_id"],
        ascending=[False, True, True],
    ).drop_duplicates(["pair_id", "site_side", "tracker_domain"])
    best_obs_map = {
        (row.pair_id, row.site_side, row.tracker_domain): {
            "session_id": int(row.session_id),
            "request_id": int(row.request_id),
            "parent_id": _normalize_node_id(row.parent_id),
            "request_url": row.request_url,
            "request_count": int(row.request_count),
        }
        for row in best_obs_df.itertuples(index=False)
    }

    pair_meta = {
        row.pair_id: {
            "country": row.country,
            "d2_etld": row.d2_etld,
            "equivalent_etld": row.equivalent_etld,
        }
        for row in pair_universe.itertuples(index=False)
    }

    rows = []
    total_pairs = len(pair_tracker_sets)
    for pair_idx, (pair_id, side_sets) in enumerate(pair_tracker_sets.items(), start=1):
        if pair_idx == 1 or pair_idx % 250 == 0 or pair_idx == total_pairs:
            print(f"[d2-chain] pair scan {pair_idx}/{total_pairs}", flush=True)

        meta = pair_meta[pair_id]
        d2_only = side_sets["D2"] - side_sets["EQUIVALENT"]
        equivalent_only = side_sets["EQUIVALENT"] - side_sets["D2"]

        for tracker_domain in d2_only:
            observed = best_obs_map.get((pair_id, "D2", tracker_domain))
            destination = best_dest_map.get((pair_id, "EQUIVALENT"))
            if observed is None or destination is None:
                continue

            rows.append(
                {
                    "pair_id": pair_id,
                    "country": meta["country"],
                    "d2_etld": meta["d2_etld"],
                    "equivalent_etld": meta["equivalent_etld"],
                    "present_side": "D2",
                    "missing_side": "EQUIVALENT",
                    "observed_etld": meta["d2_etld"],
                    "missing_etld": meta["equivalent_etld"],
                    "tracker_domain": tracker_domain,
                    "observed_session_id": observed["session_id"],
                    "observed_request_id": observed["request_id"],
                    "observed_request_parent_id": observed["parent_id"],
                    "observed_request_url": observed["request_url"],
                    "observed_session_n_requests": observed["request_count"],
                    "missing_session_id": destination["session_id"],
                    "missing_session_n_requests": destination["request_count"],
                }
            )

        for tracker_domain in equivalent_only:
            observed = best_obs_map.get((pair_id, "EQUIVALENT", tracker_domain))
            destination = best_dest_map.get((pair_id, "D2"))
            if observed is None or destination is None:
                continue

            rows.append(
                {
                    "pair_id": pair_id,
                    "country": meta["country"],
                    "d2_etld": meta["d2_etld"],
                    "equivalent_etld": meta["equivalent_etld"],
                    "present_side": "EQUIVALENT",
                    "missing_side": "D2",
                    "observed_etld": meta["equivalent_etld"],
                    "missing_etld": meta["d2_etld"],
                    "tracker_domain": tracker_domain,
                    "observed_session_id": observed["session_id"],
                    "observed_request_id": observed["request_id"],
                    "observed_request_parent_id": observed["parent_id"],
                    "observed_request_url": observed["request_url"],
                    "observed_session_n_requests": observed["request_count"],
                    "missing_session_id": destination["session_id"],
                    "missing_session_n_requests": destination["request_count"],
                }
            )

    print(f"[d2-chain] selected cases={len(rows):,}", flush=True)
    return pd.DataFrame(rows)


def export_d2_session_level_deviation_analysis(pair_universe, paired_sessions, all_requests, tracker_requests, pair_tracker_sets, output_dir):
    # Reconstruct representative request chains for D2-only and equivalent-only tracker differences.
    print("[d2-chain] start chain analysis", flush=True)
    chain_output_dir = os.path.join(output_dir, "chain_analysis")
    os.makedirs(chain_output_dir, exist_ok=True)

    cases = _select_session_level_cases(
        pair_universe,
        paired_sessions,
        all_requests,
        tracker_requests,
        pair_tracker_sets,
    )

    if cases.empty:
        pd.DataFrame(columns=[
            "pair_id", "country", "d2_etld", "equivalent_etld", "present_side", "missing_side",
            "observed_etld", "missing_etld", "tracker_domain", "observed_session_id", "missing_session_id",
            "observed_request_id", "last_matching_src", "deviation_src", "top_src_in_chain",
            "n_src_nodes_in_chain", "reason", "top_chain_node_id", "top_chain_node_type",
        ]).to_csv(f"{chain_output_dir}/session_level_chain_deviation_cases.csv", index=False)

        pd.DataFrame(columns=[
            "pair_id", "country", "d2_etld", "equivalent_etld", "present_side", "missing_side",
            "observed_session_id", "observed_request_id", "depth", "node_type", "node_id", "parent_id", "src",
        ]).to_csv(f"{chain_output_dir}/session_level_observed_inclusion_chains.csv", index=False)

        pd.DataFrame(columns=[
            "country", "present_side", "missing_side", "reason", "n_cases", "n_total_cases", "pct_cases",
            "example_top_chain_node_id",
        ]).to_csv(f"{chain_output_dir}/session_level_chain_reason_summary.csv", index=False)

        pd.DataFrame(columns=[
            "country", "present_side", "missing_side", "deviation_src", "n_cases", "n_total_cases", "pct_cases",
        ]).to_csv(f"{chain_output_dir}/session_level_chain_deviation_summary.csv", index=False)
        print("[d2-chain] no selected cases; wrote empty chain_analysis outputs", flush=True)
        return

    print("[d2-chain] loading scripts/html context...", flush=True)
    session_ids_needed = set(cases["observed_session_id"].tolist()) | set(cases["missing_session_id"].tolist())
    scripts_df, html_df = _load_dom_context_for_sessions(session_ids_needed)
    scripts_index, html_index, session_src_values = _build_parent_indexes(scripts_df, html_df)
    print(
        f"[d2-chain] indexed nodes scripts={len(scripts_index):,}, html_elements={len(html_index):,}",
        flush=True,
    )

    detail_rows = []
    chain_rows = []
    total_cases = len(cases)

    for case_idx, row in enumerate(cases.itertuples(index=False), start=1):
        if case_idx == 1 or case_idx % 1000 == 0 or case_idx == total_cases:
            print(f"[d2-chain] processing case {case_idx}/{total_cases}", flush=True)

        chain = _trace_inclusion_chain(
            row.observed_session_id,
            row.observed_request_parent_id,
            scripts_index,
            html_index,
        )

        for node in chain:
            chain_rows.append(
                {
                    "pair_id": row.pair_id,
                    "country": row.country,
                    "d2_etld": row.d2_etld,
                    "equivalent_etld": row.equivalent_etld,
                    "present_side": row.present_side,
                    "missing_side": row.missing_side,
                    "observed_session_id": row.observed_session_id,
                    "observed_request_id": row.observed_request_id,
                    "depth": node["depth"],
                    "node_type": node["node_type"],
                    "node_id": node["node_id"],
                    "parent_id": node["parent_id"],
                    "src": node["src"],
                }
            )

        src_nodes_top_down = [node for node in reversed(chain) if node["src"]]
        destination_srcs = session_src_values.get(int(row.missing_session_id), set())

        last_matching_src = ""
        deviation_src = ""
        for node in src_nodes_top_down:
            if node["src"] in destination_srcs:
                last_matching_src = node["src"]
                continue
            deviation_src = node["src"]
            break

        top_src_in_chain = src_nodes_top_down[0]["src"] if src_nodes_top_down else ""

        detail_rows.append(
            {
                "pair_id": row.pair_id,
                "country": row.country,
                "d2_etld": row.d2_etld,
                "equivalent_etld": row.equivalent_etld,
                "present_side": row.present_side,
                "missing_side": row.missing_side,
                "observed_etld": row.observed_etld,
                "missing_etld": row.missing_etld,
                "tracker_domain": row.tracker_domain,
                "observed_session_id": row.observed_session_id,
                "missing_session_id": row.missing_session_id,
                "observed_request_id": row.observed_request_id,
                "last_matching_src": last_matching_src,
                "deviation_src": deviation_src,
                "top_src_in_chain": top_src_in_chain,
                "n_src_nodes_in_chain": len(src_nodes_top_down),
            }
        )

    detail_df = pd.DataFrame(detail_rows)

    if chain_rows:
        chain_df = pd.DataFrame(chain_rows)
        top_chain_debug_df = (
            chain_df.sort_values([
                "pair_id",
                "present_side",
                "missing_side",
                "observed_session_id",
                "observed_request_id",
                "depth",
            ])
            .groupby([
                "pair_id",
                "present_side",
                "missing_side",
                "observed_session_id",
                "observed_request_id",
            ], as_index=False)
            .agg(
                top_chain_node_id=("node_id", "last"),
                top_chain_node_type=("node_type", "last"),
            )
        )
        chain_df.to_csv(f"{chain_output_dir}/session_level_observed_inclusion_chains.csv", index=False)
        print(f"[d2-chain] wrote session_level_observed_inclusion_chains.csv rows={len(chain_df):,}", flush=True)
    else:
        top_chain_debug_df = pd.DataFrame(columns=[
            "pair_id", "present_side", "missing_side", "observed_session_id", "observed_request_id",
            "top_chain_node_id", "top_chain_node_type",
        ])
        pd.DataFrame(columns=[
            "pair_id", "country", "d2_etld", "equivalent_etld", "present_side", "missing_side",
            "observed_session_id", "observed_request_id", "depth", "node_type", "node_id", "parent_id", "src",
        ]).to_csv(f"{chain_output_dir}/session_level_observed_inclusion_chains.csv", index=False)
        print("[d2-chain] wrote empty session_level_observed_inclusion_chains.csv", flush=True)

    detail_df["reason"] = detail_df.apply(
        lambda row: _classify_reason(
            int(row["n_src_nodes_in_chain"]),
            row["last_matching_src"],
            row["observed_etld"],
        ),
        axis=1,
    )
    detail_df = detail_df.merge(
        top_chain_debug_df,
        on=["pair_id", "present_side", "missing_side", "observed_session_id", "observed_request_id"],
        how="left",
    )
    detail_df.to_csv(f"{chain_output_dir}/session_level_chain_deviation_cases.csv", index=False)
    print(f"[d2-chain] wrote session_level_chain_deviation_cases.csv rows={len(detail_df):,}", flush=True)

    reason_summary = (
        detail_df.groupby(["country", "present_side", "missing_side", "reason"])
        .agg(
            n_cases=("reason", "size"),
            example_top_chain_node_id=("top_chain_node_id", "first"),
        )
        .reset_index()
    )
    reason_totals = detail_df.groupby(["country", "present_side", "missing_side"]).size().rename("n_total_cases").reset_index()
    reason_summary = reason_summary.merge(
        reason_totals,
        on=["country", "present_side", "missing_side"],
        how="left",
    )
    reason_summary["pct_cases"] = (reason_summary["n_cases"] / reason_summary["n_total_cases"] * 100).round(2)
    reason_summary = reason_summary.sort_values(["country", "present_side", "missing_side", "reason"])
    reason_summary.to_csv(f"{chain_output_dir}/session_level_chain_reason_summary.csv", index=False)
    print(f"[d2-chain] wrote session_level_chain_reason_summary.csv rows={len(reason_summary):,}", flush=True)

    summary = (
        detail_df[detail_df["deviation_src"] != ""]
        .groupby(["country", "present_side", "missing_side", "deviation_src"])
        .size()
        .rename("n_cases")
        .reset_index()
    )

    if summary.empty:
        pd.DataFrame(columns=[
            "country", "present_side", "missing_side", "deviation_src", "n_cases", "n_total_cases", "pct_cases",
        ]).to_csv(f"{chain_output_dir}/session_level_chain_deviation_summary.csv", index=False)
        print("[d2-chain] wrote empty session_level_chain_deviation_summary.csv", flush=True)
        return

    totals = detail_df.groupby(["country", "present_side", "missing_side"]).size().rename("n_total_cases").reset_index()
    summary = summary.merge(totals, on=["country", "present_side", "missing_side"], how="left")
    summary["pct_cases"] = (summary["n_cases"] / summary["n_total_cases"] * 100).round(2)
    summary = summary.sort_values(["country", "present_side", "missing_side", "n_cases", "deviation_src"], ascending=[True, True, True, False, True])
    summary.to_csv(f"{chain_output_dir}/session_level_chain_deviation_summary.csv", index=False)
    print(f"[d2-chain] wrote session_level_chain_deviation_summary.csv rows={len(summary):,}", flush=True)
