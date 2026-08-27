#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import subprocess
import threading
import traceback
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, wait
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from dotenv import load_dotenv

from utils.build_results_json_file import combine_country_category_results, derive_combined_output_name
from utils.extract_gz_files import extract_gz_files_parallel
from utils.har_parser import parse_har_for_graphml


load_dotenv()

CPU_COUNT = os.cpu_count() or 1

DEFAULT_NUM_WORKERS = max(4, min(16, CPU_COUNT // 8))
DEFAULT_ELM_WORKERS = max(8, min(16, CPU_COUNT // DEFAULT_NUM_WORKERS))

NUM_WORKERS = int(os.getenv("NUM_THREADS_PREPROCESSING", str(DEFAULT_NUM_WORKERS)))
ELM_WORKERS = int(os.getenv("NUM_THREADS_GRAPH_ELM", str(DEFAULT_ELM_WORKERS)))
TIMEOUT_SECONDS = int(os.getenv("COMMAND_TIMEOUT_SECONDS", "300"))
ELM_TIMEOUT_SECONDS = int(os.getenv("ELM_TIMEOUT_SECONDS", "300"))
PG_QUERY_RUN_PATH = "pagegraph_query/run.py"
RAW_COMMANDS = ("cookies", "scripts", "requests", "js-calls", "html")
RESULTS_OUTPUT_FILENAME = "results.json"

KEEP_TAGS = {"SCRIPT", "LINK", "parser", "IMG"}


def list_immediate_subdirs(path: str) -> List[str]:
    try:
        items = os.listdir(path)
    except FileNotFoundError:
        return []

    out = []
    for name in items:
        full = os.path.join(path, name)
        if os.path.isdir(full):
            out.append(full)
    return sorted(out)


def iter_site_dirs(base_dir: str) -> List[str]:
    return list_immediate_subdirs(base_dir)


def attempt_sort_key(path: str) -> Tuple[int, str]:
    name = os.path.basename(path)
    if name.isdigit():
        return (0, f"{int(name):09d}")
    return (1, name)


def iter_attempt_dirs(site_dir: str) -> List[str]:
    return sorted(list_immediate_subdirs(site_dir), key=attempt_sort_key)


def list_graphml_in_dir(path: str) -> List[str]:
    if not os.path.isdir(path):
        return []
    files = []
    for name in os.listdir(path):
        if name.endswith(".graphml"):
            files.append(os.path.join(path, name))
    return sorted(files)


def remove_duplicates_from_list(obj_list: List[Any]) -> List[Any]:
    seen = set()
    unique = []

    for item in obj_list:
        item_str = json.dumps(item, sort_keys=True)
        if item_str not in seen:
            seen.add(item_str)
            unique.append(item)

    return unique


def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{hashlib.sha256(path.encode()).hexdigest()[:8]}"
    with open(tmp, "w", encoding="utf-8") as file_obj:
        json.dump(data, file_obj, indent=2)
    os.replace(tmp, path)


def safe_json_load(raw_text: str) -> Any:
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw_text[start : end + 1])
        raise


def normalize_pg_output(result: Any) -> Any:
    if isinstance(result, dict):
        meta = result.get("meta", {})
        if isinstance(meta, dict):
            result["url"] = meta.get("url")

        report = result.get("report")
        if isinstance(report, list):
            result["report"] = remove_duplicates_from_list(report)

    return result


def run_pg(
    command: str,
    graphml_path: str,
    extra_args: Optional[List[str]] = None,
    timeout_seconds: Optional[float] = None,
) -> str:
    if extra_args is None:
        extra_args = []
    if timeout_seconds is None:
        timeout_seconds = TIMEOUT_SECONDS

    cmd = ["python3", PG_QUERY_RUN_PATH, command, graphml_path] + extra_args
    if command == "html":
        cmd += ["--at-serialization", "--body-content"]

    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=timeout_seconds,
    )
    return res.stdout


def run_and_parse_command(
    graphml_path: str,
    command: str,
    extra_args: Optional[List[str]] = None,
    timeout_seconds: Optional[float] = None,
) -> Any:
    raw = run_pg(command, graphml_path, extra_args, timeout_seconds=timeout_seconds)
    parsed = normalize_pg_output(safe_json_load(raw))
    return parsed


def send_command_with_timeout(
    graphml_path: str,
    command: str,
    extra_args: Optional[List[str]] = None,
    timeout_seconds: Optional[float] = None,
) -> Optional[Any]:
    try:
        return run_and_parse_command(
            graphml_path,
            command,
            extra_args=extra_args,
            timeout_seconds=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return None


def worker_label() -> str:
    return f"pid={os.getpid()} thread={threading.current_thread().name}"


def run_raw_commands_parallel(graphml_path: str) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    total_commands = len(RAW_COMMANDS)
    graph_name = os.path.basename(graphml_path)

    for idx, command in enumerate(RAW_COMMANDS, start=1):
        print(
            f"[WORKER CMD START] {worker_label()} | graph={graph_name} | "
            f"command={command} | {idx}/{total_commands}"
        )
        results[command] = run_and_parse_command(graphml_path, command)

    return results


def get_request_edge_ids(requests_obj: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    for req_item in requests_obj.get("report", []):
        request_block = req_item.get("request", {})
        request_obj = request_block.get("request", {})
        request_edge_id = request_obj.get("id")
        if request_edge_id:
            ids.add(request_edge_id)
    return ids


def get_script_node_ids(scripts_obj: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    for script_item in scripts_obj.get("report", []):
        script = script_item.get("script", {})
        script_id = script.get("id")
        if script_id:
            ids.add(script_id)
    return ids


def extract_elements_from_html(html_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    elements = html_obj["report"]["elements"]
    if not isinstance(elements, list):
        raise RuntimeError("html.report.elements is not a list")

    kept = []
    for element in elements:
        if not isinstance(element, dict):
            continue
        tag = element.get("tag", "")
        if tag in KEEP_TAGS:
            kept.append(element)
    return kept


def get_html_element_ids(html_obj: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    for element in extract_elements_from_html(html_obj):
        element_id = element.get("id")
        if element_id:
            ids.add(element_id)
    return ids


def build_elm_cache(graphml_path: str, element_ids: Iterable[str]) -> Dict[str, Any]:
    unique_ids = sorted(set(element_ids))
    if not unique_ids:
        return {}

    cache: Dict[str, Any] = {}
    total_elm = len(unique_ids)
    graph_name = os.path.basename(graphml_path)
    milestone = max(1, total_elm // 10)

    executor = ThreadPoolExecutor(max_workers=min(ELM_WORKERS, len(unique_ids)))
    try:
        futures = {
            executor.submit(run_and_parse_command, graphml_path, "elm", [element_id]): element_id
            for element_id in unique_ids
        }

        done, not_done = wait(futures, timeout=ELM_TIMEOUT_SECONDS)

        for completed_idx, future in enumerate(as_completed(done), start=1):
            element_id = futures[future]
            cache[element_id] = future.result()
            if completed_idx % milestone == 0:
                print(
                    f"[WORKER ELM] {worker_label()} | graph={graph_name} | "
                    f"{completed_idx}/{total_elm} ({100 * completed_idx // total_elm}%)"
                )

        if not_done:
            for future in not_done:
                future.cancel()
            print(
                f"[WORKER ELM TIMEOUT] {worker_label()} | graph={graph_name} | "
                f"completed={len(done)}/{total_elm} | timed_out_after={ELM_TIMEOUT_SECONDS}s"
            )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return cache


def submit_elm_cache_jobs(
    elm_executor: ThreadPoolExecutor,
    graphml_path: str,
    element_ids: Iterable[str],
    timeout_seconds: float,
) -> Dict[Future, str]:
    unique_ids = sorted(set(element_ids))
    if not unique_ids:
        return {}

    futures: Dict[Future, str] = {}
    for element_id in unique_ids:
        future = elm_executor.submit(
            send_command_with_timeout,
            graphml_path,
            "elm",
            [element_id],
            timeout_seconds,
        )
        futures[future] = element_id

    return futures


def extract_requester_from_request_elm(elm_obj: Dict[str, Any]) -> Tuple[Any, Any]:
    try:
        incoming = elm_obj["report"]["incoming node"]
        requester_id = incoming.get("id", -1)
        requester_type = incoming.get("type", -1)
        return (
            requester_id if requester_id is not None else -1,
            requester_type if requester_type is not None else -1,
        )
    except KeyError as exc:
        raise KeyError("Missing report->incoming node in request elm output") from exc


def derive_requests_to_scripts(requests_obj: Dict[str, Any], elm_cache: Dict[str, Any]) -> None:
    for req_item in requests_obj.get("report", []):
        request_block = req_item.get("request", {})
        request_obj = request_block.get("request", {})
        request_edge_id = request_obj.get("id")
        if not request_edge_id:
            continue
        elm_obj = elm_cache.get(request_edge_id)
        if elm_obj is None:
            continue
        requester_id, requester_type = extract_requester_from_request_elm(elm_obj)
        req_item["requester_id"] = requester_id
        req_item["requester_type"] = requester_type
        # Backward-compatibility with previous schema.
        req_item["parent_id"] = requester_id


def extract_script_lineage_from_script_elm(elm_obj: Dict[str, Any]) -> Dict[str, Any]:
    incoming_edges = elm_obj["report"].get("incoming edges", [])

    creator_id = -1
    creator_type = -1
    executor_id = -1
    executor_type = -1

    for edge in incoming_edges:
        if edge.get("type") == "create node":
            incoming_node = edge.get("incoming node", {})
            creator_id = incoming_node.get("id", -1)
            creator_type = incoming_node.get("type", -1)
            if creator_id is None:
                creator_id = -1
            if creator_type is None:
                creator_type = -1
            break

    for edge in incoming_edges:
        if edge.get("type") == "execute":
            incoming_node = edge.get("incoming node", {})
            executor_id = incoming_node.get("id", -1)
            executor_type = incoming_node.get("type", -1)
            if executor_id is None:
                executor_id = -1
            if executor_type is None:
                executor_type = -1
            break

    parent_id = creator_id if creator_id != -1 else executor_id

    return {
        "creator_id": creator_id,
        "creator_type": creator_type,
        "executor_id": executor_id,
        "executor_type": executor_type,
        "parent_id": parent_id,
    }


def derive_scripts_to_loader_scripts(scripts_obj: Dict[str, Any], elm_cache: Dict[str, Any]) -> None:
    for script_item in scripts_obj.get("report", []):
        script = script_item.get("script", {})
        script_id = script.get("id")
        if not script_id:
            print(f"[WARN] {worker_label()} | malformed script item without script.id; skipping")
            continue

        # Prefer values already present in the scripts command output.
        executor_obj = script.get("executor")
        if not isinstance(executor_obj, dict):
            executor_obj = {}
        if "executor_id" not in script_item:
            script_item["executor_id"] = executor_obj.get("id", -1)
        if "executor_type" not in script_item:
            script_item["executor_type"] = executor_obj.get("type", -1)

        elm_obj = elm_cache.get(script_id)
        if elm_obj is None:
            continue
        lineage = extract_script_lineage_from_script_elm(elm_obj)
        script_item["creator_id"] = lineage["creator_id"]
        script_item["creator_type"] = lineage["creator_type"]

        # Fill only if scripts output did not already provide executor fields.
        if script_item.get("executor_id", -1) == -1:
            script_item["executor_id"] = lineage["executor_id"]
        if script_item.get("executor_type", -1) == -1:
            script_item["executor_type"] = lineage["executor_type"]

        # Backward-compatibility with previous schema.
        script_item["parent_id"] = lineage["parent_id"]


def find_parent_from_elem(elem_obj: Dict[str, Any]) -> Dict[str, Any]:
    incoming_edges = elem_obj["report"].get("incoming edges", [])
    if not isinstance(incoming_edges, list):
        return {
            "creator_id": -1,
            "creator_type": -1,
            "dom_parent_id": -1,
            "dom_parent_type": -1,
        }

    creator_id = -1
    creator_type = -1
    dom_parent_id = -1
    dom_parent_type = -1

    for edge in incoming_edges:
        if not isinstance(edge, dict):
            continue
        if edge.get("type") == "create node":
            incoming_node = edge.get("incoming node", {})
            creator_id = incoming_node.get("id", -1)
            creator_type = incoming_node.get("type", -1)
            if creator_id is None:
                creator_id = -1
            if creator_type is None:
                creator_type = -1
            break

    for edge_type in ("document", "insert node", "structure"):
        for edge in incoming_edges:
            if not isinstance(edge, dict):
                continue
            if edge.get("type") != edge_type:
                continue
            incoming_node = edge.get("incoming node", {})
            dom_parent_id = incoming_node.get("id", -1)
            dom_parent_type = incoming_node.get("type", -1)
            if dom_parent_id is None:
                dom_parent_id = -1
            if dom_parent_type is None:
                dom_parent_type = -1
            break
        if dom_parent_id != -1:
            break

    return {
        "creator_id": creator_id,
        "creator_type": creator_type,
        "dom_parent_id": dom_parent_id,
        "dom_parent_type": dom_parent_type,
    }


def derive_html_elements(site_name: str, html_obj: Dict[str, Any], elm_cache: Dict[str, Any]) -> List[Dict[str, Any]]:
    graph_url = html_obj["meta"]["url"]
    elements = extract_elements_from_html(html_obj)
    rows = []

    for element in elements:
        element_id = element["id"]
        tag = element["tag"]
        attrs = element.get("attrs", {})

        src = ""
        if tag == "SCRIPT":
            src = attrs.get("src", "")
        elif tag == "LINK":
            src = attrs.get("href", "")

        elem_obj = elm_cache.get(element_id)
        if elem_obj is None:
            lineage = {
                "creator_id": -1,
                "creator_type": -1,
                "dom_parent_id": -1,
                "dom_parent_type": -1,
            }
        else:
            lineage = find_parent_from_elem(elem_obj)

        rows.append(
            {
                "site": site_name,
                "url": graph_url,
                "src": src,
                "attrs": attrs,
                "tag": tag,
                "id": element_id,
                "creator_id": lineage["creator_id"],
                "creator_type": lineage["creator_type"],
                "dom_parent_id": lineage["dom_parent_id"],
                "dom_parent_type": lineage["dom_parent_type"],
                # Backward-compatibility with previous schema.
                "parent_id": lineage["creator_id"],
                "parent_type": lineage["creator_type"],
            }
        )

    return rows


def collect_graphml_raw_data(graphml_path: str, get_elements_details: bool = True) -> Tuple[Dict[str, Any], Set[str]]:
    command_results = run_raw_commands_parallel(graphml_path)

    required_elm_ids: Set[str] = set()
    if get_elements_details:
        required_elm_ids.update(get_request_edge_ids(command_results["requests"]))
        required_elm_ids.update(get_script_node_ids(command_results["scripts"]))
        required_elm_ids.update(get_html_element_ids(command_results["html"]))

    return command_results, required_elm_ids


def build_graphml_summary(
    site_name: str,
    graphml_path: str,
    command_results: Dict[str, Any],
    elm_cache: Optional[Dict[str, Any]] = None,
    get_elements_details: bool = True,
) -> Dict[str, Any]:
    if elm_cache is None:
        elm_cache = {}

    if get_elements_details:
        derive_requests_to_scripts(command_results["requests"], elm_cache)
        derive_scripts_to_loader_scripts(command_results["scripts"], elm_cache)

    html_rows = derive_html_elements(site_name, command_results["html"], elm_cache)
    graph_url = command_results["html"]["meta"]["url"]

    return {
        "url": graph_url,
        "graphml_path": graphml_path,
        "cookies": command_results["cookies"]["report"],
        "scripts": command_results["scripts"]["report"],
        "requests": command_results["requests"]["report"],
        "js-calls": command_results["js-calls"],
        "html_elements": html_rows,
        "data_from_har": parse_har_for_graphml(graphml_path),
    }


def process_attempt_dir(site_name: str, attempt_dir: str, get_elements_details: bool = True) -> Optional[Dict[str, Any]]:
    graphml_files = list_graphml_in_dir(attempt_dir)
    if not graphml_files:
        return None

    results_path = os.path.join(attempt_dir, RESULTS_OUTPUT_FILENAME)
    if os.path.isfile(results_path):
        return None

    attempt_id = os.path.basename(attempt_dir)
    graph_items: List[Dict[str, Any]] = []

    for graphml_path in graphml_files:
        command_results, required_elm_ids = collect_graphml_raw_data(graphml_path, get_elements_details=get_elements_details)
        graph_items.append(
            {
                "graphml_path": graphml_path,
                "command_results": command_results,
                "required_elm_ids": required_elm_ids,
            }
        )

    return {
        "site": site_name,
        "attempt_id": attempt_id,
        "attempt_dir": attempt_dir,
        "graph_items": graph_items,
    }


def process_attempt_artifact(attempt_artifact: Dict[str, Any], get_elements_details: bool = True) -> None:
    results_path = os.path.join(attempt_artifact["attempt_dir"], RESULTS_OUTPUT_FILENAME)
    data = []

    for graph_item in attempt_artifact["graph_items"]:
        data.append(
            build_graphml_summary(
                attempt_artifact["site"],
                graph_item["graphml_path"],
                graph_item["command_results"],
                elm_cache=graph_item.get("elm_cache", {}),
                get_elements_details=get_elements_details,
            )
        )

    write_json(
        results_path,
        {
            "site": attempt_artifact["site"],
            "attempt_id": attempt_artifact["attempt_id"],
            "data": data,
        },
    )



def process_one_site(site_dir: str, elm_executor: ThreadPoolExecutor, get_elements_details: bool = True) -> None:
    site_name = os.path.basename(site_dir)
    try:
        attempt_artifacts: List[Dict[str, Any]] = []

        for attempt_dir in iter_attempt_dirs(site_dir):
            artifact = process_attempt_dir(site_name, attempt_dir, get_elements_details=get_elements_details)
            if artifact is not None:
                attempt_artifacts.append(artifact)

        if get_elements_details:
            all_futures: Dict[Future, Tuple[Dict[str, Any], str]] = {}
            total_elm = sum(
                len(graph_item["required_elm_ids"])
                for attempt_artifact in attempt_artifacts
                for graph_item in attempt_artifact["graph_items"]
            )

            for attempt_artifact in attempt_artifacts:
                for graph_item in attempt_artifact["graph_items"]:
                    graphml_path = graph_item["graphml_path"]
                    elm_futures = submit_elm_cache_jobs(
                        elm_executor,
                        graphml_path,
                        graph_item["required_elm_ids"],
                        ELM_TIMEOUT_SECONDS,
                    )
                    graph_item["elm_cache"] = {}
                    for future, element_id in elm_futures.items():
                        all_futures[future] = (graph_item, element_id)

            completed_elm = 0
            timed_out_elm = 0
            for future in as_completed(all_futures):
                graph_item, element_id = all_futures[future]
                elm_result = future.result()
                if elm_result is None:
                    timed_out_elm += 1
                else:
                    graph_item["elm_cache"][element_id] = elm_result
                completed_elm += 1
                print(
                    f"[SITE ELM] {worker_label()} | site={site_name} | "
                    f"{completed_elm}/{total_elm} ({100 * completed_elm // total_elm}%) "
                    f"| timed_out={timed_out_elm}"
                )

            print(
                f"[SITE ELM DONE] {worker_label()} | site={site_name} | "
                f"completed={completed_elm}/{total_elm} | timed_out={timed_out_elm}"
            )

        for attempt_artifact in attempt_artifacts:
            process_attempt_artifact(attempt_artifact, get_elements_details=get_elements_details)

    except Exception as e:
        print(f"\n[WORKER FAIL] {site_name} | {e}")
        traceback.print_exc()
        raise  




def process_all_sites(
    base_dir: str,
    min_index: int = 0,
    max_index: int = None,
    get_elements_details: bool = True,
) -> None:
    site_dirs = list(iter_site_dirs(base_dir))
    if min_index or max_index is not None:
        site_dirs = site_dirs[min_index:max_index]
    total_sites = len(site_dirs)
    print(f"Base dir: {base_dir}")
    print(f"Sites found: {total_sites}")
    print(f"Site workers: {NUM_WORKERS}")
    print("Structure: site -> attempt_id -> graphml files")
    print("Processing mode: external pagegraph_query CLI only")
    print(f"Raw commands: {list(RAW_COMMANDS)}")
    print("Raw commands execution: sequential per graph")
    print(f"Get elements details via elm: {get_elements_details}")
    print(f"Site workers (threads): {NUM_WORKERS}")
    print(f"Shared elm workers (threads): {ELM_WORKERS}")
    print()

    with ThreadPoolExecutor(max_workers=max(1, ELM_WORKERS)) as elm_executor:
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as site_executor:
            futures = {
                site_executor.submit(process_one_site, site_dir, elm_executor, get_elements_details): site_dir
                for site_dir in site_dirs
            }
            completed_sites = 0
            for future in as_completed(futures):
                site_dir = futures[future]
                site_name = os.path.basename(site_dir)
                completed_sites += 1

                try:
                    future.result()
                    print(f"[OK] {site_name} | {completed_sites}/{total_sites} sites")
                except Exception as exc:
                    print(f"[FAIL] {site_name} | {exc} | {completed_sites}/{total_sites} sites")
                    traceback.print_exc()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process PageGraph files using the external pagegraph_query CLI and build one country/category aggregate."
    )
    parser.add_argument(
        "--base-dir",
        help="Root snapshots directory, e.g. ../../data/snapshots/DZ/country_coded",
    )
    parser.add_argument(
        "--min-index",
        type=int,
        default=0,
        help="Start index (inclusive) into the sorted list of site dirs (default: 0)",
    )
    parser.add_argument(
        "--max-index",
        type=int,
        default=None,
        help="End index (exclusive) into the sorted list of site dirs (default: all)",
    )
    parser.add_argument(
        "--skip-combine",
        action="store_true",
        help="Skip the final combine step (use when running a partial slice)",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip the initial .gz extraction step",
    )
    parser.add_argument(
        "--combine-only",
        action="store_true",
        help="Skip extraction and site processing; run only the final combine step",
    )
    parser.add_argument(
        "--get-elements-details",
        default="true",
        choices=("true", "false"),
        help="When true, run elm commands to derive requests/scripts/html lineage details (default: true)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = os.path.abspath(args.base_dir)
    get_elements_details = args.get_elements_details == "true"

    if not os.path.isdir(base_dir):
        print(f"Base directory not found: {base_dir}")
        return 1

    combined_output_name = derive_combined_output_name(base_dir)

    if args.combine_only:
        print("Combine-only mode enabled: skipping extraction and site processing.\n")
        out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/files_to_analyze"))
        os.makedirs(out_dir, exist_ok=True)
        combine_country_category_results(base_dir, os.path.join(out_dir, combined_output_name))
        print("Finished: Combining country/category results.\n")
        return 0

    if not args.skip_extract:
        print("Starting: Extracting .gz files...")
        extract_gz_files_parallel(base_dir)
        print("Finished: .gz files extracted.\n")
    else:
        print("Skipping .gz extraction (--skip-extract).\n")

    print("Starting: Processing PageGraph data...")
    print(f"Index range: [{args.min_index}, {args.max_index})")
    process_all_sites(
        base_dir,
        min_index=args.min_index,
        max_index=args.max_index,
        get_elements_details=get_elements_details,
    )
    print("Finished: Processing PageGraph data.\n")

    if not args.skip_combine:
        print("Starting: Combining country/category results...")
        out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/files_to_analyze"))
        os.makedirs(out_dir, exist_ok=True)
        combine_country_category_results(base_dir, os.path.join(out_dir, combined_output_name))
        print("Finished: Combining country/category results.\n")
    else:
        print("Skipping combine step (--skip-combine).\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())