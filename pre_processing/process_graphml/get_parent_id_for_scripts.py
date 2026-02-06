import os
import json
import subprocess
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv
from typing import Optional, Any, Dict, List, Tuple

load_dotenv()

NUM_WORKERS = int(os.getenv("NUM_THREADS_PREPROCESSING", "1"))  # site-level parallelism
PG_QUERY_RUN_PATH = "pagegraph_query/run.py"
timeout_seconds = 1800

# CHANGE THIS
base_dir = "../../data/snapshots/US/global"

# OUTPUT PER SITE
output_filename = "scripts_to_loader_scripts.json"


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
    return sorted(out, reverse=True)


def list_graphml_in_dir(path: str) -> List[str]:
    """Only *.graphml directly inside `path` (non-recursive)."""
    if not os.path.isdir(path):
        return []
    files = []
    for name in os.listdir(path):
        if name.endswith(".graphml"):
            files.append(os.path.join(path, name))
    return sorted(files)


def run_pg(command: str, graphml_path: str, extra_args: Optional[List[str]] = None) -> str:
    if extra_args is None:
        extra_args = []
    cmd = ["python3", PG_QUERY_RUN_PATH, command, graphml_path] + extra_args
    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=timeout_seconds,
    )
    return res.stdout


def safe_json_load(s: str) -> Any:
    """
    Some tools print logs before JSON; try to recover by slicing from first '{' to last '}'.
    """
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(s[start : end + 1])
        raise


def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{hashlib.sha256(path.encode()).hexdigest()[:8]}"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


# -----------------------------
# Scripts -> loader script id
# -----------------------------

def get_script_data(graph_url: str, script_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    script_item is one element from `scripts` report:
      { "script": {...}, "frame": {...} }
    """
    s = script_item["script"]

    return {
        "page_url": graph_url,
        "script_id": s['id'],
        "script_type": s['type'],
        "script_hash": s['hash'],
    }


def extract_loader_script_id_from_script_elm(elm_obj: Dict[str, Any]) -> Any:
    """
    From script-elm output:
      look in report -> incoming edges
      if edge.type == "create node": return edge["incoming node"]["id"]
      else -1
    """
    report = elm_obj["report"]
    incoming_edges = report["incoming edges"]

    for e in incoming_edges:
        if e["type"] == "create node":
            incoming_node = e["incoming node"]
            loader_id = incoming_node["id"]
            if loader_id:
                return loader_id 

    for e in incoming_edges:
        if e["type"] == "execute":
            incoming_node = e["incoming node"]
            loader_id = incoming_node["id"]
            if loader_id:
                return loader_id

    return -1


def process_graphml_scripts(graphml_path: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    """
    Sequential per-graphml:
      - run scripts
      - for each script: run elm(graphml, script_id)
      - extract loader_script_id from incoming 'create node' edge, else -1
    Returns: (graph_url, [entries...])
    """
    scripts_stdout = run_pg("scripts", graphml_path)
    scripts_obj = safe_json_load(scripts_stdout)

    graph_url = scripts_obj["meta"]["url"]
    report = scripts_obj["report"]

    entries = []
    if len(report) == 0:
        print('0 scripts', 'passing')
        return (graph_url, entries)


    print("number of scripts:", len(report))

    for script_item in report:
        entry = get_script_data(graph_url, script_item)
        sid = entry["script_id"]

        if not sid:
            raise RuntimeError(
                f"Script ID not found"
            ) 

        elm_stdout = run_pg("elm", graphml_path, [sid])
        elm_obj = safe_json_load(elm_stdout)

  
        entry["parent_script_id"] = extract_loader_script_id_from_script_elm(elm_obj)
       # entry["elm_raw"] = elm_obj 

        entries.append(entry)

    return (graph_url, entries)



def build_mapping_for_dir(graphml_files: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    builds { url: [entries...], url2: [entries...] }
    """
    mapping: Dict[str, List[Dict[str, Any]]] = {}

    for f in graphml_files:
        print("process_graphml_scripts:", f)
        url, entries = process_graphml_scripts(f)

        if entries is None:
            raise RuntimeError(f"[ERROR] No entries found in graphml file: {f}")

        if not url:
            url = ""

        mapping.setdefault(url, []).extend(entries)

    return mapping


def process_one_site(site_dir: str) -> None:
    """
    Writes:
      site_dir/scripts_to_loader_scripts.json
      site_dir/validation/scripts_to_loader_scripts.json (if enabled)
    """
    main_out = os.path.join(site_dir, output_filename)
    if os.path.isfile(main_out):
        return

    main_graphml = list_graphml_in_dir(site_dir)
    main_mapping = build_mapping_for_dir(main_graphml)
    write_json(main_out, main_mapping)

    # validation (kept commented like your original)
    # val_dir = os.path.join(site_dir, "validation")
    # if os.path.isdir(val_dir):
    #     val_graphml = list_graphml_in_dir(val_dir)
    #     val_out = os.path.join(val_dir, output_filename)
    #     val_mapping = build_mapping_for_dir(val_graphml)
    #     write_json(val_out, val_mapping)


def get_scripts_to_loader_scripts_per_site() -> None:
    site_dirs = list_immediate_subdirs(base_dir)
    site_dirs = [d for d in site_dirs if os.path.basename(d) != "validation"]
    #site_dirs = site_dirs[8:9]

    print(f"Base dir: {base_dir}")
    print(f"Sites found: {len(site_dirs)}")
    print(f"Site workers: {NUM_WORKERS}")
    print()

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futures = {ex.submit(process_one_site, sd): sd for sd in site_dirs}
        for fut in as_completed(futures):
            site_dir = futures[fut]
            try:
                fut.result()
            except Exception as e:
                print(f"[FAIL] {os.path.basename(site_dir)} | {e}")


# -----------------------------
# Merge per-site JSON -> one flat list
# -----------------------------

def iter_site_dirs(direct: str) -> List[str]:
    """Immediate subdirs of direct, excluding a top-level 'validation' dir if present."""
    site_dirs = list_immediate_subdirs(direct)
    return [d for d in site_dirs if os.path.basename(d) != "validation"]


def _merge_one_site(site_dir: str, validation: bool) -> Tuple[str, int, List[Dict[str, Any]]]:
    """Worker: read one site's scripts_to_loader_scripts.json and return flattened rows."""
    site_name = os.path.basename(site_dir)

    json_path = (
        os.path.join(site_dir, "validation", output_filename)
        if validation
        else os.path.join(site_dir, output_filename)
    )

    if not os.path.isfile(json_path):
        return (site_name, 0, [])

    try:
        with open(json_path, "r") as f:
            mapping = json.load(f)  # expected: { url: [entries...] }
    except Exception as e:
        return (site_name, 0, [{"site": site_name, "error": f"cannot read {json_path}: {e}"}])

    if not isinstance(mapping, dict):
        return (site_name, 0, [{"site": site_name, "error": f"unexpected JSON shape: {json_path}"}])

    out: List[Dict[str, Any]] = []
    for url, entries in mapping.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if entry.get("error"):
                continue

            out.append(
                {
                    "site": site_name,
                    "url": url,
                    "script_id": entry.get("script_id"),
                    "script_type": entry.get("script_type"),
                    "script_hash": entry.get("script_hash"),
                    "script_url": entry.get("script_url"),
                    "executor_id": entry.get("executor_id"),
                    "executor_tag": entry.get("executor_tag"),
                    "executor_attrs": entry.get("executor_attrs"),
                    "frame_id": entry.get("frame_id"),
                    "frame_url": entry.get("frame_url"),
                    "frame_security_origin": entry.get("frame_security_origin"),
                    "frame_blink_id": entry.get("frame_blink_id"),
                    "frame_main": entry.get("frame_main"),
                    "loader_script_id": entry.get("loader_script_id", -1),
                }
            )

    return (site_name, len(out), out)


def merge_scripts_to_loader_scripts_parallel(
    direct: str,
    output_file: str,
    validation: bool = False,
    num_workers: Optional[int] = None,
) -> str:
    """
    Parallel merge of per-site scripts_to_loader_scripts.json into one flat list.
    Writes to ../../data/files_to_analyze/<output_file>.
    """
    if num_workers is None:
        num_workers = NUM_WORKERS

    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/files_to_analyze"))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, output_file)

    site_dirs = iter_site_dirs(direct)
    print(f"[MERGE] base_dir={direct} validation={validation} sites={len(site_dirs)} workers={num_workers}")

    merged: List[Dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=num_workers) as ex:
        futures = {ex.submit(_merge_one_site, sd, validation): sd for sd in site_dirs}

        done_sites = 0
        for fut in as_completed(futures):
            done_sites += 1
            site_name = os.path.basename(futures[fut])
            try:
                sname, nrows, rows = fut.result()
                merged.extend(rows)
                if done_sites % 50 == 0:
                    print(
                        f"[MERGE] {done_sites}/{len(site_dirs)} sites | last={sname} "
                        f"added={nrows} | total_rows={len(merged)}"
                    )
            except Exception as e:
                print(f"[MERGE-FAIL] {site_name} | {e}")

    write_json(out_path, merged)
    print(f"[MERGE-OK] validation={validation} | rows={len(merged)} | out={out_path}")
    return out_path


if __name__ == "__main__":
     get_scripts_to_loader_scripts_per_site()

     merge_scripts_to_loader_scripts_parallel(
        "../../data/snapshots/US/global",
        "SCRIPTS_TO_LOADER_SCRIPTS_US_GLOBAL.json",
        validation=False,
    )