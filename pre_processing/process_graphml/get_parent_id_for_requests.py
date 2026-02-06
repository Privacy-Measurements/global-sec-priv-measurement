import os
import json
import subprocess
import hashlib
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()
 
NUM_WORKERS = int(os.getenv("NUM_THREADS_PREPROCESSING", "1"))  # now used for sites
#NUM_WORKERS = 1  # now used for sites
PG_QUERY_RUN_PATH = "pagegraph_query/run.py"
timeout_seconds = 1800  
base_dir = "../../data/snapshots/DE/global"

output_filename = "requests_to_scripts.json"


def list_immediate_subdirs(path):
    try:
        items = os.listdir(path)
    except FileNotFoundError:
        return []
    out = []
    for name in items:
        full = os.path.join(path, name)
        if os.path.isdir(full):
            out.append(full)

    random.shuffle(out)
    return out

#    return sorted(out, reverse = True)


def list_graphml_in_dir(path: str):
    """Only *.graphml directly inside `path` (non-recursive)."""
    if not os.path.isdir(path):
        return []
    files = []
    for name in os.listdir(path):
        if name.endswith(".graphml"):
            files.append(os.path.join(path, name))
    return sorted(files)


def run_pg(command: str, graphml_path: str, extra_args=None):
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


def safe_json_load(s: str):
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(s[start : end + 1])
        raise


def extract_script_id_from_elm(elm_obj: dict):
    """
    Extract script node id from elm output.
    Assumes this format:
      elm_obj["report"]["incoming node"]["id"]
    """
    try:
        return elm_obj["report"]["incoming node"]["id"]
    except KeyError as e:
        raise KeyError(
            f"Missing report→incoming node→id in elm output.\n"
            f"Top-level keys: {list(elm_obj.keys())}"
        )


def get_request_data(graph_url, req_item):

#    print('1')
    request_block = req_item["request"]
#    print('2')

    request_id = request_block["request id"] 
#    print('3')

    request_obj = request_block["request"]
#    print('4')

    request_url = request_obj["url"]
#    print('5')

    request_edge_id = request_obj["id"]  # e.g., "e119"
#    print('6')

    if 'result' in request_block:
        result_obj = request_block["result"]
        result_hash = result_obj.get('hash', '')
        result_size = result_obj.get("size", 0)
        result_status = result_obj["status"]
    
    else:
        result_hash = ''
        result_size = 0
        result_status = ''


    frame_obj = req_item["frame"]
#    print('11')

    frame_id = frame_obj["id"]  # e.g., "n32"
#    print('12')


    return {
        "page_url": graph_url,
        "request_url": request_url,
        "request_id": request_id,
        "request_edge_id": request_edge_id,
        "result_hash": result_hash,
        "result_size": result_size,
        "result_status": result_status,
        "frame_id": frame_id,
    }


def process_graphml(graphml_path: str):
    """
    Sequential per-graphml:
      - run requests
      - for each request: run elm
    Returns: (graph_url, [entries...])
    """

        
    requests_stdout = run_pg("requests", graphml_path)
    requests_obj = json.loads(requests_stdout)
    graph_url = requests_obj["meta"]["url"]
    report = requests_obj["report"]
    
    entries = []
    if len(report) == 0:
        return (graph_url, entries)


    print('number of requests:', len(report))

#    if len(report) >100:
#        raise RuntimeError(
#            f"Too long"
#           ) 

    cpt = 0
    for req_item in report:
        #print(cpt)
        cpt+=1

#            if cpt <= 220: 
#                continue 

#            print('build entry')
        entry = get_request_data(graph_url, req_item)
#            print('entry done')
        request_edge_id = entry["request_edge_id"]
#            print('request_edge_id done')
        
        elm_stdout = run_pg("elm", graphml_path, [request_edge_id])
#            print('elm_stdout done')

        elm_obj = safe_json_load(elm_stdout)
#            print('elm_obj done')

        entry["script_id"] = extract_script_id_from_elm(elm_obj)
#            print('script_id done')

        entry["elm_raw"] = elm_obj            
        entries.append(entry)
#            print('entries appended')

    return (graph_url, entries)


def write_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{hashlib.sha256(path.encode()).hexdigest()[:8]}"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)



def build_mapping_for_dir(graphml_files):
    """
    builds { url: [entries...], url2: [entries...] }
    """
    mapping = {}

    for f in graphml_files:
        
        print('process_graphml:', f)
        url, entries = process_graphml(f)
#        print(url)
 #       print(entries)
 #       print(entries is None)
        if entries is None:
            raise RuntimeError(
                f"[ERROR] No entries found in graphml file: {f}"
            )

        if not url:
            url = ''

        mapping.setdefault(url, []).extend(entries)


    return mapping



def process_one_site(site_dir: str):
    """
    Writes:
      site_dir/requests_to_scripts.json
      site_dir/validation/requests_to_scripts.json (if exists)
    """
    main_out = os.path.join(site_dir, output_filename)
    if os.path.isfile(main_out):
        return

    main_graphml = list_graphml_in_dir(site_dir)
    main_mapping = build_mapping_for_dir(main_graphml)
    write_json(main_out, main_mapping)

    # validation
 #   val_dir = os.path.join(site_dir, "validation")
  #  if os.path.isdir(val_dir):
   #     val_graphml = list_graphml_in_dir(val_dir)
   #     val_out = os.path.join(val_dir, output_filename)
   #     val_mapping = build_mapping_for_dir(val_graphml)
   #     write_json(val_out, val_mapping)


def get_scripts_to_requests_per_site():
    site_dirs = list_immediate_subdirs(base_dir)
    site_dirs = [d for d in site_dirs if os.path.basename(d) != "validation"]
   
#    site_dirs = site_dirs[2000:3000]
#    site_dirs = [os.path.join(base_dir, 'www.savana.com')]

    print(f"Base dir: {base_dir}")
    print(f"Sites found: {len(site_dirs)}")
    print(f"Site workers: {NUM_WORKERS}")
    print()

    results = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as ex:
        futures = {ex.submit(process_one_site, sd): sd for sd in site_dirs}

        for fut in as_completed(futures):
            site_dir = futures[fut]  
            try:
                fut.result() 
               # print(f"[OK] {os.path.basename(site_dir)}")
            except Exception as e:
                print(f"[FAIL] {os.path.basename(site_dir)} | {e}")





def iter_site_dirs(direct: str):
    """Immediate subdirs of base_dir, excluding a top-level 'validation' dir if present."""
    site_dirs = list_immediate_subdirs(direct)
    return [d for d in site_dirs if os.path.basename(d) != "validation"]


def _merge_one_site(site_dir: str, validation: bool):
    """Worker: read one site's requests_to_scripts.json and return flattened rows."""
    site_name = os.path.basename(site_dir)

    json_path = (
        os.path.join(site_dir, "validation", output_filename)
        if validation
        else os.path.join(site_dir, output_filename)
    )

    if not os.path.isfile(json_path):
        return (site_name, 0, [])  # (site, rows, data)

    try:
        with open(json_path, "r") as f:
            mapping = json.load(f)  # expected: { url: [entries...] }
    except Exception as e:
        return (site_name, 0, [{"site": site_name, "error": f"cannot read {json_path}: {e}"}])

    if not isinstance(mapping, dict):
        return (site_name, 0, [{"site": site_name, "error": f"unexpected JSON shape: {json_path}"}])

    out = []
    for url, entries in mapping.items():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if entry.get("error"):  # skip per-graphml error objects
                continue

            out.append(
                {
                    "site": site_name,
                    "url": url,
                    "request_url": entry.get("request_url"),
                    "request_id": entry.get("request_id"),
                    "request_edge_id": entry.get("request_edge_id"),
                    "result_hash": entry.get("result_hash"),
                    "result_size": entry.get("result_size"),
                    "result_status": entry.get("result_status"),
                    "frame_id": entry.get("frame_id"),
                    "script_id": entry.get("script_id"),
                }
            )

    return (site_name, len(out), out)


def merge_requests_to_scripts_parallel(
    direct: str,
    output_file: str,
    validation: bool = False,
    num_workers: int | None = None,
):
    """
    Parallel merge of per-site requests_to_scripts.json into one flat list.
    Writes to ../../data/files_to_analyze/<output_file>.
    """
    if num_workers is None:
        num_workers = NUM_WORKERS  # reuse your env-based setting :contentReference[oaicite:1]{index=1}

    out_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../data/files_to_analyze")
    )
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, output_file)

    site_dirs = iter_site_dirs(direct)  # :contentReference[oaicite:2]{index=2}
    print(f"[MERGE] base_dir={direct} validation={validation} sites={len(site_dirs)} workers={num_workers}")

    merged = []
    with ProcessPoolExecutor(max_workers=num_workers) as ex:
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




if __name__ == "__main__":
    get_scripts_to_requests_per_site()

   # merge_requests_to_scripts_parallel('../../data/snapshots/US/global', "REQUESTS_TO_SCRIPTS_US_GLOBAL.json", validation=False)
 #   merge_requests_to_scripts_parallel('../../data/snapshots/IN/global', "REQUESTS_TO_SCRIPTS_IN_GLOBAL.json", validation=False)
  #  merge_requests_to_scripts_parallel('../../data/snapshots/DZ/global', "REQUESTS_TO_SCRIPTS_DZ_GLOBAL.json", validation=False)
#    merge_requests_to_scripts_parallel('../../data/snapshots/DE/global', "REQUESTS_TO_SCRIPTS_DE_GLOBAL.json", validation=False)

    # merge validation
#    merge_requests_to_scripts(base_dir, "merged_requests_to_scripts_validation.json", validation=True)

