import json, os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from analysis.utils.urls import extract_domain_from_url, is_http_url, normalize_domain


def _normalize_node_id(value):
    # IDs are expected to already be in canonical form like n12345.
    if value is None or pd.isna(value):
        return None

    raw = str(value).strip()
    if not raw:
        return None
    return raw


def _safe_src_from_attrs(attrs_json_value):
    # Extract src from attrs_json whether it is already a dict or a JSON-encoded string.
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


def _classify_reason(n_src_nodes_in_chain, last_matching_src, etld):
    # Apply the user-defined logic for first_party/third_party attribution.
    if n_src_nodes_in_chain == 0:
        return "first_party"

    if not last_matching_src:
        return "first_party"

    if not is_http_url(last_matching_src):
        return "first_party"

    src_domain = normalize_domain(extract_domain_from_url(last_matching_src))
    etld_domain = normalize_domain(etld)
    if not src_domain or not etld_domain:
        return "first_party"

    if src_domain == etld_domain:
        return "first_party"

    return "third_party"


def _load_dom_context_for_sessions(session_ids):
    # Load scripts/html_elements only for sessions involved in the session-level investigation.
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

    # Pull both possible parent-node tables used by request.parent_id traversal.
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

# Build O(1) lookup dictionaries and per-session src inventories.
def _build_parent_indexes(scripts_df, html_df):
    scripts_index = {}
    html_index = {}
    session_src_values = {}

    # Index script nodes by (session_id, script_id).
    for row in scripts_df.itertuples(index=False):
        sid = int(row.session_id)
        script_id = _normalize_node_id(row.script_id)
        parent_id = _normalize_node_id(row.parent_id)
        if not script_id:
            continue
        src = "" if pd.isna(row.script_src) else str(row.script_src)
        scripts_index[(sid, script_id)] = {
            "parent_id": parent_id,
            "src": src,
            "node_type": "script",
        }
        if src:
            session_src_values.setdefault(sid, set()).add(src)

    # Index html element nodes by (session_id, element_id).
    for row in html_df.itertuples(index=False):
        sid = int(row.session_id)
        element_id = _normalize_node_id(row.element_id)
        parent_id = _normalize_node_id(row.parent_id)
        if not element_id:
            continue
        src = _safe_src_from_attrs(row.attrs_json)
        html_index[(sid, element_id)] = {
            "parent_id": parent_id,
            "src": src,
            "node_type": "html_element",
        }
        if src:
            session_src_values.setdefault(sid, set()).add(src)

    return scripts_index, html_index, session_src_values


def _trace_inclusion_chain(session_id, start_parent_id, scripts_index, html_index):
    # Follow parent_id recursively through scripts/html_elements until no parent can be resolved
    chain = []
    current_id = _normalize_node_id(start_parent_id)
    if not current_id:
        return chain

    seen = set()
    depth = 0

    # Use a visited set as a safety guard against parent cycles
    while current_id not in seen:
        seen.add(current_id)

        key = (int(session_id), current_id)
        # A parent_id may refer to either table; check scripts first, then html elements.
        node = scripts_index.get(key)
        if node is None:
            node = html_index.get(key)

        if node is None:
            # Keep a terminal marker when parent cannot be resolved in either table.
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


# Build (etld, tracker, present_country, missing_country) rows with session_ids
def _select_session_level_cases(strict_sessions, strict_all_requests, strict_tracker_requests, tracker_sets, countries):
    # Request counts are used to choose a representative "largest" session per case.
    print("[session-level] selecting representative cases...", flush=True)
    req_counts = strict_all_requests.groupby("session_id").size().to_dict()

    # Build one unique session metadata table for fast joins/lookups.
    session_meta = strict_sessions[["session_id", "etld", "location"]].drop_duplicates().copy()
    session_meta["request_count"] = session_meta["session_id"].map(lambda sid: req_counts.get(sid, 0))

    # Precompute the best destination session per (etld, country): max request_count, then smallest session_id.
    best_dest_df = session_meta.sort_values(
        ["request_count", "session_id"],
        ascending=[False, True],
    ).drop_duplicates(["etld", "location"])
    best_dest_map = {
        (row.etld, row.location): (int(row.session_id), int(row.request_count))
        for row in best_dest_df.itertuples(index=False)
    }

    # Normalize tracker requests once, attach etld/country, then keep best observed request per (etld, country, tracker).
    print("[session-level] preprocessing tracker requests...", flush=True)
    tracker_df = strict_tracker_requests.merge(
        session_meta[["session_id", "etld", "location", "request_count"]],
        on="session_id",
        how="inner",
    )
    tracker_df["tracker_domain"] = tracker_df["request_url"].apply(extract_domain_from_url)
    tracker_df = tracker_df.dropna(subset=["tracker_domain"])

    best_obs_df = tracker_df.sort_values(
        ["request_count", "session_id", "request_id"],
        ascending=[False, True, True],
    ).drop_duplicates(["etld", "location", "tracker_domain"])

    best_obs_map = {
        (row.etld, row.location, row.tracker_domain): {
            "session_id": int(row.session_id),
            "request_id": int(row.request_id),
            "parent_id": _normalize_node_id(row.parent_id),
            "request_url": row.request_url,
            "request_count": int(row.request_count),
        }
        for row in best_obs_df.itertuples(index=False)
    }

    print(
        f"[session-level] precomputed best observed keys={len(best_obs_map):,}, best destination keys={len(best_dest_map):,}",
        flush=True,
    )

    rows = []
    total_etlds = len(tracker_sets)

    for etld_idx, (etld, per_country) in enumerate(tracker_sets.items(), start=1):
        if etld_idx == 1 or etld_idx % 250 == 0 or etld_idx == total_etlds:
            print(f"[session-level] etld scan {etld_idx}/{total_etlds}", flush=True)

        for country_present in countries:
            present_set = per_country[country_present]
            if not present_set:
                continue

            for country_missing in countries:
                if country_missing == country_present:
                    continue

                diff_trackers = present_set - per_country[country_missing]
                if not diff_trackers:
                    continue

                destination = best_dest_map.get((etld, country_missing))
                if destination is None:
                    continue

                destination_session_id, destination_req_count = destination

                for tracker_domain in diff_trackers:
                    observed = best_obs_map.get((etld, country_present, tracker_domain))
                    if observed is None:
                        continue

                    rows.append(
                        {
                            "etld": etld,
                            "tracker_domain": tracker_domain,
                            "country_present": country_present,
                            "country_missing": country_missing,
                            "observed_session_id": observed["session_id"],
                            "observed_request_id": observed["request_id"],
                            "observed_request_parent_id": observed["parent_id"],
                            "observed_request_url": observed["request_url"],
                            "observed_session_n_requests": observed["request_count"],
                            "missing_session_id": destination_session_id,
                            "missing_session_n_requests": destination_req_count,
                        }
                    )

    print(f"[session-level] selected cases={len(rows):,}", flush=True)
    return pd.DataFrame(rows)


# Investigate where inclusion-chain sources diverge between observed and non-observed countries.
def export_session_level_deviation_analysis(strict_sessions, strict_all_requests, strict_tracker_requests, tracker_sets, output_dir, countries):
    # Build a list of cases (tracker observed in one country, but not in another) with session ids
    print("[session-level] start deviation analysis", flush=True)
    chain_output_dir = os.path.join(output_dir, "chain_analysis")
    os.makedirs(chain_output_dir, exist_ok=True)
    cases = _select_session_level_cases(strict_sessions, strict_all_requests, strict_tracker_requests, tracker_sets, countries)


    # Load html_elements and scripts for selected sessions
    print("[session-level] loading scripts/html context...", flush=True)
    session_ids_needed = set(cases["observed_session_id"].tolist()) | set(cases["missing_session_id"].tolist())
    scripts_df, html_df = _load_dom_context_for_sessions(session_ids_needed)
    print(f"[session-level] sessions with dom context={len(session_ids_needed):,}", flush=True)
    scripts_index, html_index, session_src_values = _build_parent_indexes(scripts_df, html_df)
    print(
        f"[session-level] indexed nodes scripts={len(scripts_index):,}, html_elements={len(html_index):,}",
        flush=True,
    )

    detail_rows = []
    chain_rows = []

    # Build observed-side chain and compare its src nodes against destination session sources
    total_cases = len(cases)
    for case_idx, row in enumerate(cases.itertuples(index=False), start=1):
        if case_idx == 1 or case_idx % 1000 == 0 or case_idx == total_cases:
            print(f"[session-level] processing case {case_idx}/{total_cases}", flush=True)

        chain = _trace_inclusion_chain(
            row.observed_session_id,
            row.observed_request_parent_id,
            scripts_index,
            html_index,
        )

        for node in chain:
            chain_rows.append(
                {
                    "etld": row.etld,
                    "tracker_domain": row.tracker_domain,
                    "country_present": row.country_present,
                    "country_missing": row.country_missing,
                    "observed_session_id": row.observed_session_id,
                    "observed_request_id": row.observed_request_id,
                    "depth": node["depth"],
                    "node_type": node["node_type"],
                    "node_id": node["node_id"],
                    "parent_id": node["parent_id"],
                    "src": node["src"],
                }
            )

        # Compare from highest ancestor with src toward the request-side ancestors.
        src_nodes_top_down = [n for n in reversed(chain) if n["src"]]
        destination_srcs = session_src_values.get(int(row.missing_session_id), set())

        last_matching_src = ""
        deviation_src = ""
        # Record the last src still found in destination and the first src that deviates.
        for node in src_nodes_top_down:
            if node["src"] in destination_srcs:
                last_matching_src = node["src"]
                continue
            deviation_src = node["src"]
            break

        top_src_in_chain = src_nodes_top_down[0]["src"] if src_nodes_top_down else ""

        detail_rows.append(
            {
                "etld": row.etld,
                "tracker_domain": row.tracker_domain,
                "country_present": row.country_present,
                "country_missing": row.country_missing,
                "observed_session_id": row.observed_session_id,
                "missing_session_id": row.missing_session_id,
                "observed_request_id": row.observed_request_id,
                "last_matching_src": last_matching_src,
                "deviation_src": deviation_src,
                "top_src_in_chain": top_src_in_chain,
                "n_src_nodes_in_chain": len(src_nodes_top_down),
            }
        )

    # Write detailed output and full observed chains.
    detail_df = pd.DataFrame(detail_rows)
    detail_df.to_csv(f"{chain_output_dir}/session_level_chain_deviation_cases.csv", index=False)
    print(f"[session-level] wrote session_level_chain_deviation_cases.csv rows={len(detail_df):,}", flush=True)

    if chain_rows:
        pd.DataFrame(chain_rows).to_csv(f"{chain_output_dir}/session_level_observed_inclusion_chains.csv", index=False)
        print(f"[session-level] wrote session_level_observed_inclusion_chains.csv rows={len(chain_rows):,}", flush=True)
    else:
        pd.DataFrame(columns=["etld", "tracker_domain", "country_present", "country_missing", "observed_session_id", "observed_request_id", "depth", "node_type", "node_id", "parent_id", "src"]).to_csv(
            f"{chain_output_dir}/session_level_observed_inclusion_chains.csv",
            index=False,
        )
        print("[session-level] wrote empty session_level_observed_inclusion_chains.csv", flush=True)

    # Add reason attribution and top-chain debug id for each case.
    detail_df["reason"] = detail_df.apply(
        lambda r: _classify_reason(
            int(r["n_src_nodes_in_chain"]),
            r["last_matching_src"],
            r["etld"],
        ),
        axis=1,
    )

    top_chain_debug_df = (
        pd.DataFrame(chain_rows)
        .sort_values(["etld", "tracker_domain", "country_present", "country_missing", "observed_session_id", "observed_request_id", "depth"]) 
        .groupby(["etld", "tracker_domain", "country_present", "country_missing", "observed_session_id", "observed_request_id"], as_index=False)
        .agg(
            top_chain_node_id=("node_id", "last"),
            top_chain_node_type=("node_type", "last"),
        )
    ) if chain_rows else pd.DataFrame(
        columns=["etld", "tracker_domain", "country_present", "country_missing", "observed_session_id", "observed_request_id", "top_chain_node_id", "top_chain_node_type"]
    )

    detail_df = detail_df.merge(
        top_chain_debug_df,
        on=["etld", "tracker_domain", "country_present", "country_missing", "observed_session_id", "observed_request_id"],
        how="left",
    )

    # Rewrite detail file with newly added reason/debug columns.
    detail_df.to_csv(f"{chain_output_dir}/session_level_chain_deviation_cases.csv", index=False)

    # Summary of first/third-party attribution per ordered country pair.
    reason_summary = (
        detail_df.groupby(["country_present", "country_missing", "reason"])
        .agg(
            n_cases=("reason", "size"),
            example_top_chain_node_id=("top_chain_node_id", "first"),
        )
        .reset_index()
    )
    reason_totals = detail_df.groupby(["country_present", "country_missing"]).size().rename("n_total_cases").reset_index()
    reason_summary = reason_summary.merge(reason_totals, on=["country_present", "country_missing"], how="left")
    reason_summary["pct_cases"] = (reason_summary["n_cases"] / reason_summary["n_total_cases"] * 100).round(2)
    reason_summary = reason_summary.sort_values(["country_present", "country_missing", "reason"])
    reason_summary.to_csv(f"{chain_output_dir}/session_level_chain_reason_summary.csv", index=False)
    print(f"[session-level] wrote session_level_chain_reason_summary.csv rows={len(reason_summary):,}", flush=True)

    # Aggregate top deviation sources per ordered country pair.
    summary = (
        detail_df[detail_df["deviation_src"] != ""]
        .groupby(["country_present", "country_missing", "deviation_src"])
        .size()
        .rename("n_cases")
        .reset_index()
    )

    if summary.empty:
        pd.DataFrame(columns=["country_present", "country_missing", "deviation_src", "n_cases", "n_total_cases", "pct_cases"]).to_csv(
            f"{chain_output_dir}/session_level_chain_deviation_summary.csv",
            index=False,
        )
        print("[session-level] wrote empty session_level_chain_deviation_summary.csv", flush=True)
        return


    # Compute pair-specific percentages over all selected cases for that ordered pair.
    totals = detail_df.groupby(["country_present", "country_missing"]).size().rename("n_total_cases").reset_index()
    summary = summary.merge(totals, on=["country_present", "country_missing"], how="left")
    summary["pct_cases"] = (summary["n_cases"] / summary["n_total_cases"] * 100).round(2)
    summary = summary.sort_values(["country_present", "country_missing", "n_cases", "deviation_src"], ascending=[True, True, False, True])
    summary.to_csv(f"{chain_output_dir}/session_level_chain_deviation_summary.csv", index=False)
    print(f"[session-level] wrote session_level_chain_deviation_summary.csv rows={len(summary):,}", flush=True)
