import os
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv


load_dotenv()
NUM_THREADS = int(os.getenv("NUM_THREADS_PREPROCESSING", 1))  

def extract_gz_file(gz_path):
    """Extract a single .gz file."""
    extracted_path = gz_path[:-3]  # Remove .gz extension
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(extracted_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz_path)
        return f"{gz_path} done"
    except Exception as e:
        return f"Failed to process {gz_path}: {e}"

def extract_gz_files_parallel(root_dir):
    """Extract all .gz files in the directory tree in parallel."""
    gz_files = []

    # Collect all .gz files
    for foldername, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.gz'):
                gz_files.append(os.path.join(foldername, filename))

    if not gz_files:
        print("No .gz files found.")
        return

    # Parallel extraction
    print(f"Using {NUM_THREADS} parallel threads for extraction...")  # <-- added line

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(extract_gz_file, gz_path) for gz_path in gz_files]
        for future in as_completed(futures):
            print(future.result())