from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from utils.extract_gz_files import extract_gz_files_parallel
from utils.build_results_json_file import build_results_json_for_each_etld_parallel, combine_all_etld_jsons
from dotenv import load_dotenv
import subprocess, json, os, hashlib
import glob

load_dotenv()

NUM_THREADS = int(os.getenv("NUM_THREADS_PREPROCESSING", 1))  

PG_QUERY_RUN_PATH = "pagegraph_query/run.py"


base_dir = '../../data/snapshots/US/global'


def remove_duplicates_from_list(obj_list):
    seen = set()
    unique = []

    for item in obj_list:

        item_str = json.dumps(item, sort_keys=True)
        if item_str not in seen:
            seen.add(item_str)
            unique.append(item)

    return unique


def write_results_file(file_path, data, cmd):

    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
    except (OSError, FileNotFoundError) as e:

        print(f"Failed to open file '{file_path}': {e}")
        print("Falling back to hashed filename...")

        fallback_path = get_hashed_file_path(file_path, cmd)

        try:
            with open(fallback_path, 'w') as f:
                json.dump(data, f, indent=4)
    
        except Exception as fallback_error:
            print(f"Fallback also failed: {fallback_error}")


def get_hashed_file_path(file_path, cmd):

    directory, filename = os.path.split(file_path)

    hashed_name = hashlib.sha256(filename.encode()).hexdigest()[:10]
    hashed_filename = f"output_{hashed_name}.{cmd}.json"
    hashed_path = os.path.join(directory, hashed_filename)

    return hashed_path


def get_all_graphml_files(base_path):
    result = []
    dir_count = 0

    for root, dirs, files in os.walk(base_path):

        for file in files:
            if file.endswith('.graphml'):
                full_path = os.path.join(root, file)
                size = os.path.getsize(full_path)
                result.append((full_path, size))

        dir_count += 1


    result.sort(key=lambda x: x[1], reverse=True)

    a = sorted([file_path for file_path, _ in result])
    return a



def run_pagegraph_cli(command, input_path):

    cmd = ["python3", PG_QUERY_RUN_PATH, command, input_path]

    if command == "html":
        cmd += ["--at-serialization", "--body-content"]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=1800  # max 30 minutes

        )
        return result.stdout

    except subprocess.TimeoutExpired as e:
        raise 

    except subprocess.CalledProcessError as e:
        raise



def process_file(file, cmds):

    
    output_file_path = file[:-8]  # remove .pagegraph or similar

    for cmd in cmds:

        output_file_path_for_cmd = f"{output_file_path}.{cmd}.json"

        if os.path.exists(output_file_path_for_cmd) or os.path.exists(get_hashed_file_path(output_file_path_for_cmd, cmd)):
            continue

        print("Processing:", file, cmd)

        try:
            res = run_pagegraph_cli(cmd, file)
            res = json.loads(res)

            res["url"] = res["meta"]["url"]
            res["report"] = remove_duplicates_from_list(res["report"])

            write_results_file(output_file_path_for_cmd, res, cmd)

        except Exception as e:
            print(f"[ERROR] {cmd} failed for {file}: {e}")



def extract_data_from_pagegraph(base_dir, data_types):

    files = get_all_graphml_files(base_dir)                                                                                                                                                                                                                                        
    num_workers = min(NUM_THREADS, os.cpu_count()) 

    print(f"Using {num_workers} Threads")                                                                                                                                                                                                                                  
    print(f"Number of files: {len(files)}")                                                                                                                                                                                                                                  

                                                                                                                                                                                                                                                                           
    with ProcessPoolExecutor(max_workers=num_workers) as executor:                                                                                                                                                                                                          
        futures = [                                                                                                                                                                                                                                                        
            executor.submit(process_file, file, data_types)                                                                                                                                                                                                             
            for file in files                                                                                                                                                                                                                              
        ]                                                                                                                                                                                                                                                                  

        for future in as_completed(futures):                                                                                                                                                                                                                               
            future.result()                                                                                                                                                                                                                                                



if __name__ == "__main__":
    

    print("Starting: Extracting .gz files...")
    extract_gz_files_parallel(base_dir)
    print("Finished: .gz files extracted.\n")

    print("Starting: Extracting data from pagegraph...")
    extract_data_from_pagegraph(base_dir, ['cookies', 'scripts', 'requests', 'js-calls'])
    print("Finished: Extracting data from pagegraph...")


    print("Starting: Building results JSON file...")
    build_results_json_for_each_etld_parallel(base_dir, int(NUM_THREADS/2))

    combine_all_etld_jsons(base_dir, '../../data/files_to_analyze/US_global.json')
    combine_all_etld_jsons(base_dir, '../../data/files_to_analyze/US_global_validation.json', validation=True)




 
