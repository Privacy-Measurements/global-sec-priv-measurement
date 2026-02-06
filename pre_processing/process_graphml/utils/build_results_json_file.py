import os
import json
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

def process_single_etld(etld_dir, base_directory, output_file_name):

    
    #path to ETLD directory
    etld_path = os.path.join(base_directory, etld_dir)

    # What measures to put in the JSON
    required_measures = ['cookies', 'scripts', 'requests', 'js-calls']
    
    # Check if the report already exists
    output_path = os.path.join(etld_path, output_file_name)
    if os.path.exists(output_path):
        return 

        
    # validate number of files ---
    graphml_files = [f for f in os.listdir(etld_path) if f.endswith('.graphml')]
    num_graphml = len(graphml_files)

    for measure in required_measures:
        measure_files = [f for f in os.listdir(etld_path) if f.endswith(f".{measure}.json")]
        if len(measure_files) != num_graphml:
            # If counts differ, skip processing this ETLD
            print(f"Skipping {etld_path}: number of .{measure}.json ({len(measure_files)}) != number of .graphml ({num_graphml})")
            return
    # --- End of new validation ---


    # Get all JSON files in the directory
    json_files = [f for f in os.listdir(etld_path) if f.endswith('.json') and f != 'report.json']

    # Group files by hash string
    hash_files = defaultdict(dict)    
    for filename in json_files:
        match = re.match(r'(.+)_(\d+)\.(.+)\.json$', filename)
        if match:
            hash_string = match.group(1)
            timestamp = match.group(2)
            measure = match.group(3)
            
            if measure in required_measures:
                hash_files[hash_string][measure] = filename
    
    # Filter hash strings that have all required measures
    valid_hashes = {}
    for hash_string, files in hash_files.items():
        if all(measure in files for measure in required_measures):
            valid_hashes[hash_string] = files
        else:
            return
    
    
    # Create JSON

    print(f"Processing {etld_path}...")

    consolidated_data = {}
    
    for hash_string, files in valid_hashes.items():
        try:
            # Read cookies file to get URL
            cookies_file = os.path.join(etld_path, files['cookies'])
            with open(cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            
            url_value = cookies_data.get('meta', {}).get('url', f'unknown')
            
            # Read all measure files
            hash_data = {}
            for measure in required_measures:
                measure_file = os.path.join(etld_path, files[measure])
                with open(measure_file, 'r', encoding='utf-8') as f:
                    hash_data[measure] = json.load(f)
            
            consolidated_data[url_value] = hash_data
            
        except Exception as e:

            
            print(f"  Error processing {hash_string}: {e}")
            for measure_file in files.values():
                file_path = os.path.join(etld_path, measure_file)
                #if os.path.exists(file_path):
                #    try:
                #        os.remove(file_path)
                #        print(f"    Deleted {file_path}")
                #    except Exception as rm_e:
                #        print(f"    Could not delete {file_path}: {rm_e}")

            return
    
    # Write consolidated JSON file
    if consolidated_data:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(consolidated_data, f, indent=2, ensure_ascii=False)
        
    else:
        print(f"No valid data found for {etld_path}")




def build_results_json_for_each_etld_parallel(base_directory, max_workers=None):
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()
 
    etld_directories = [
        d for d in os.listdir(base_directory) 
        if os.path.isdir(os.path.join(base_directory, d))
    ]


    print(f"Processing {len(etld_directories)} eTLD directories (x2 each) using {max_workers} workers...")


    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for etld_dir in sorted(etld_directories):

            output_file_name = f"{etld_dir}.json"
            # eTLD call
            futures.append(executor.submit(process_single_etld, etld_dir, base_directory, output_file_name))
            # validation call
            validation_path = os.path.join(base_directory, etld_dir)
            validation_dir = os.path.join(validation_path, "validation")

            if os.path.isdir(validation_dir):
                files_in_validation = [f for f in os.listdir(validation_dir) if f != "crawl.log"]
                if files_in_validation:
                    futures.append(executor.submit(process_single_etld, "validation", validation_path, output_file_name))

        # Wait for all tasks to finish
        for future in as_completed(futures):
            try:
                future.result()  
            except Exception as exc:
                print(f"Task generated an exception: {exc}")

    print("Processing complete!")



def combine_all_etld_jsons(base_directory, output_file, validation = False):
    """Combine all individual eTLD JSON files into one big JSONL file"""

    processed_etlds = set()

    if os.path.exists(output_file):
        tmp_file = output_file + ".tmp"

        print(f"Resuming from existing {output_file}...")
        prev_line = None
        with open(output_file, "r", encoding="utf-8") as infile, open(tmp_file, "w", encoding="utf-8") as outfile:
            for line in infile:
                if prev_line is not None:
                    # Keep everything except the last line
                    outfile.write(prev_line)
                    try:
                        obj = json.loads(prev_line)
                        processed_etlds.add(obj["etld"])
                        print('Found:', obj["etld"])
                    except Exception:
                        pass
                prev_line = line  

        os.replace(tmp_file, output_file)  


    etld_directories = [d for d in os.listdir(base_directory) 
                       if os.path.isdir(os.path.join(base_directory, d))]
    
    processed_count = len(processed_etlds)
    
    print(f"Combining {len(etld_directories)} eTLD JSON files...")
    print(f"Skipping {processed_count} already processed.")

    with open(output_file, 'a', encoding='utf-8') as out_f:
        for etld_dir in etld_directories:

            if etld_dir in processed_etlds:
                continue  # already done

            if validation:
                validation_dir = os.path.join(base_directory, etld_dir, f"validation")
                etld_json_file = os.path.join(validation_dir, f"{etld_dir}.json")
            
            else:
                etld_json_file = os.path.join(base_directory, etld_dir, f"{etld_dir}.json")
            
            
            if os.path.exists(etld_json_file):
                try:
                    with open(etld_json_file, 'r', encoding='utf-8') as f:
                        etld_data = json.load(f)
                    
                    record = {
                        "etld": etld_dir,
                        "data": etld_data
                    }
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    
                    processed_count += 1
                    print(f"  Added {etld_dir} ({len(etld_data)} URLs)")
                    
                except Exception as e:
                    print(f"  Error reading {etld_dir}.json: {e}")
            else:
                print(f"  No JSON file found for {etld_dir}")
    
    print(f"\nCombined JSONL data written to {output_file}")
    print(f"Processed {processed_count} eTLD files")

