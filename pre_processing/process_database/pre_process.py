
# pre_process.py
import subprocess

scripts = [
    "utils/extract_session_and_local_storage_from_js_calls.py",
    "utils/parse_storage_values.py"
    "utils/identify_uid_values.py",
    "utils/label_tracking_requests.py",
#    "utils/cname_resolution.py"
]

for script in scripts:
    print(f"Running {script}...")
    process = subprocess.Popen(
        ["python", "-u", script],  
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,            
    )

    for line in process.stdout:
        print(line, end="")     

    ret = process.wait()
    if ret != 0:
        print(f"{script} exited with status {ret}")


        
print("Pre-processing completed.")