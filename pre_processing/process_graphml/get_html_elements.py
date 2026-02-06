import os
import json
import hashlib
import random
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

NUM_WORKERS = int(os.getenv("NUM_THREADS_PREPROCESSING", "1"))  
PG_QUERY_RUN_PATH = "pagegraph_query/run.py"
TIMEOUT_SECONDS = 1800  

BASE_DIR = "../../data/snapshots/US/global"
OUTPUT_FILENAME = "html_elements.json"

KEEP_TAGS = {"SCRIPT", "LINK", "parser", "IMG"}  


def list_immediate_subdirs(path: str):
    try:
        items = os.listdir(path)
    except FileNotFoundError:
        return []
    out = []
    for name in items:
        full = os.path.join(path, name)
        if os.path.isdir(full):
            out.append(full)

    return out


def list_graphml_in_dir(path: str):
    if not os.path.isdir(path):
        return []
    files = []
    for name in os.listdir(path):
        if name.endswith(".graphml"):
            files.append(os.path.join(path, name))
    return sorted(files)


def run_pg(command, graphml_path, extra_args=None):
    if extra_args is None:
        extra_args = []

    cmd = ["python3", PG_QUERY_RUN_PATH, command, graphml_path] + extra_args

    res = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=TIMEOUT_SECONDS,
    )
    return res.stdout


def safe_json_load(s: str):
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        raise


def write_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{hashlib.sha256(path.encode()).hexdigest()[:8]}"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def iter_site_dirs(base_dir: str):
    site_dirs = list_immediate_subdirs(base_dir)
    res = [d for d in site_dirs if os.path.basename(d) != "validation"]
    return sorted(res, reverse=True)


def extract_elements_from_html(html_obj: dict):

    elements = html_obj["report"]["elements"]


    if not isinstance(elements, list):
        raise Exception("html.report.elements is not a list")

    kept = []
    for el in elements:

        
        if not isinstance(el, dict):
            continue

        tag = el.get("tag", '')

        if tag in KEEP_TAGS:
            kept.append(el)
    return kept


def find_parent_from_elem(elem_obj: dict):

    incoming_edges = elem_obj["report"]["incoming edges"]
    if not isinstance(incoming_edges, list):
        return (-1, -1)

    for edge in incoming_edges:
        if not isinstance(edge, dict):
            continue
        if edge["type"] == "create node":
            incoming_node = edge["incoming node"]
            parent_id = incoming_node["id"]
            parent_type = incoming_node["type"]
            return (parent_id if parent_id is not None else -1,
                    parent_type if parent_type is not None else -1)

    return (-1, -1)


def process_graphml(site_name, graphml_path):

    rows = []
    html_stdout = run_pg("html", graphml_path)
    html_obj = safe_json_load(html_stdout)


    url = html_obj["meta"]["url"]
    elements = extract_elements_from_html(html_obj)

    for el in elements:
        el_id = el["id"]
        tag = el["tag"]
        attrs = el.get("attrs", {})

        if tag == "SCRIPT":
            src = attrs.get("src", '') 

        elif tag == "LINK":
            src = attrs.get("href", '') 
        

        elem_stdout = run_pg("elm", graphml_path, [el_id])
        elem_obj = safe_json_load(elem_stdout)
        parent_id, parent_type = find_parent_from_elem(elem_obj)

        rows.append(
            {
                "site": site_name,
                "url": url,
                "src": src,
                "attrs": attrs,
                "tag": tag,
                "id": el_id,
                "parent_id": parent_id,
                "parent_type": parent_type,
            }
        )

    return rows


def process_one_site(site_dir: str):

    site_name = os.path.basename(site_dir)
    out_path = os.path.join(site_dir, OUTPUT_FILENAME)

    if os.path.isfile(out_path):
        return

    graphml_files = list_graphml_in_dir(site_dir)
    all_rows = []
    for g in graphml_files:
        print(f"[{site_name}] html+elm: {os.path.basename(g)}")
        rows = process_graphml(site_name, g)
        all_rows.extend(rows)

    write_json(out_path, all_rows)



####################################### MERGE #################################################################3

def _merge_one_site(site_dir, validation):
    site_name = os.path.basename(site_dir)

    json_path = (
        os.path.join(site_dir, "validation", OUTPUT_FILENAME)
        if validation
        else os.path.join(site_dir, OUTPUT_FILENAME)
    )

    if not os.path.isfile(json_path):
        return (site_name, 0, [])  

    with open(json_path, "r") as f:
        entries = json.load(f)  # this is already a list


    return site_name, len(entries), entries



def merge_html_elements_parallel(direct, output_file, validation = False):

    out_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../data/files_to_analyze")
    )
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, output_file)

    site_dirs = iter_site_dirs(direct)  # :contentReference[oaicite:2]{index=2}
    print(f"[MERGE] base_dir={direct} validation={validation} sites={len(site_dirs)} workers={NUM_WORKERS}")

    merged = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futures = {ex.submit(_merge_one_site, sd, validation): sd for sd in site_dirs}

        done_sites = 0
        for fut in as_completed(futures):
            done_sites += 1
            site_name = os.path.basename(futures[fut])
            try:
                sname, nrows, rows = fut.result()
                merged.extend(rows)
                # lightweight progress (SLURM-friendly)
                if done_sites % 50 == 0:
                    print(f"[MERGE] {done_sites}/{len(site_dirs)} sites | last={sname} added={nrows} | total_rows={len(merged)}")
            except Exception as e:
                print(f"[MERGE-FAIL] {site_name} | {e}")

    write_json(out_path, merged)
    print(f"[MERGE-OK] validation={validation} | rows={len(merged)} | out={out_path}")
    return out_path


####################################### MERGE #################################################################3



def main():
    site_dirs = iter_site_dirs(BASE_DIR)

    print(f"Base dir: {BASE_DIR}")
    print(f"Sites found: {len(site_dirs)}")
    print(f"Site workers: {NUM_WORKERS}")
    print(f"Output per site: {OUTPUT_FILENAME}")
    print()


    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futures = {ex.submit(process_one_site, sd): sd for sd in site_dirs}
        for fut in as_completed(futures):
            site_dir = futures[fut]
            site_name = os.path.basename(site_dir)
            try:
                fut.result()
            except Exception as e:
                print(f"[FAIL] {site_name} | {e}")


if __name__ == "__main__":
#    main()
#   merge_html_elements_parallel('../../data/snapshots/US/global', "HTML_ELEMENTS_GLOBAL_US.json", validation=False)
#    merge_html_elements_parallel('../../data/snapshots/IN/global', "HTML_ELEMENTS_GLOBAL_IN.json", validation=False)
#    merge_html_elements_parallel('../../data/snapshots/DZ/global', "HTML_ELEMENTS_GLOBAL_DZ.json", validation=False)
    merge_html_elements_parallel('../../data/snapshots/DE/global', "HTML_ELEMENTS_GLOBAL_DE.json", validation=False)
