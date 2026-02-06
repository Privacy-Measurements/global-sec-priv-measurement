import os
import hashlib
import pymysql
import argparse
import signal
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from adblockparser import AdblockRules
from config import db_params

# Global variable for sharing rules across worker processes
_rule_objects = None


class TimeoutException(Exception): pass


def handler(signum, frame):
    raise TimeoutException()



def safe_should_block(rule_objects, url, timeout):
    """Call rule_objects.should_block(url) with a timeout (default 5s)."""

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout)

    try:
        return rule_objects.should_block(url)

    except TimeoutException:
#        print(f"[Timeout] should_block() took too long for URL: {url}")
        return None  # or False, depending on how you want to handle it
        
    except Exception as e:
        print(f"[Error] {url}: {e}")
        return None

    finally:
        signal.alarm(0)
            
def get_blocker_rules_objects():
    """Load and parse all adblock rule files once."""
    rules_dir = 'privacy_lists'
    rule_files_to_skip = {"indian_list.txt", "ru_adlist.txt"}

    all_rules = set()

    for file_name in os.listdir(rules_dir):
        if not file_name.endswith(".txt") or file_name in rule_files_to_skip:
            continue

        file_path = os.path.join(rules_dir, file_name)
        try:
            with open(file_path, encoding='utf-8', errors='ignore') as f:
                raw_rules = set(f.read().splitlines())
                if raw_rules:
                    print(f"Loading from {file_name}")
                    all_rules.update(raw_rules)
                    print(f"Loaded {len(raw_rules):,} rules from {file_name}")
        except Exception:
            print(f"Error reading {file_name}")

    print(f"Total rule sets loaded: {len(all_rules):,}")
    return AdblockRules(
        list(all_rules), 
        use_re2=True,
        skip_unsupported_rules=True  # remove all rules with $options
    )


def compute_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def identify_new_urls(db_params):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

  #  cursor.execute("""
  #      SELECT DISTINCT r.request_url
  #      FROM requests r
  #      JOIN crawl_sessions s ON r.session_id = s.id
  #      WHERE r.is_tracker IS NULL
  #      AND s.category NOT LIKE '%-validation';
  #  """)
    
    cursor.execute("""
        SELECT DISTINCT request_url
        FROM requests
        WHERE is_tracker IS NULL;
    """)
    

    rows = cursor.fetchall()
    print(f"Found {len(rows)} candidate URLs to classify.")

    count = 0
    for row in rows:
        url = row['request_url']
        url_hash = compute_url_hash(url)

        cursor.execute("""
            INSERT IGNORE INTO url_tracking_classification (url, url_hash)
            VALUES (%s, %s);
        """, (url, url_hash))

        count += 1
        if count % 1000 == 0:
            conn.commit()
            print(f"Inserted {count}/{len(rows)} URLs...")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Done. Total new URLs inserted: {count}")


def get_non_treated_urls(db_params):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT id, url FROM url_tracking_classification WHERE is_tracker IS NULL;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def init_worker():
    """Initializer for worker processes — set global rule object."""
    global _rule_objects
    _rule_objects = get_blocker_rules_objects()



def process_chunk(url_chunk, db_params, timeout, thread_id):
    """Worker function to classify a chunk of URLs."""
    global _rule_objects
    rule_objects = _rule_objects

    conn = pymysql.connect(**db_params)
    cursor = conn.cursor()

    total = len(url_chunk)
    count = 0
    updates = []

    for row in url_chunk:
        url = row['url']
        is_tracker = safe_should_block(rule_objects, url, timeout)
        
        if is_tracker is None:
            is_tracker = False 
            #continue
        
        updates.append((is_tracker, row['id']))

        count += 1
        if count % 100 == 0:
            cursor.executemany(
                "UPDATE url_tracking_classification SET is_tracker = %s WHERE id = %s;",
                updates
            )
            conn.commit()
            print(f"[Worker {thread_id}] Treated {count}/{total}")
            updates.clear()

    if updates:
        cursor.executemany(
            "UPDATE url_tracking_classification SET is_tracker = %s WHERE id = %s;",
            updates
        )
        conn.commit()
        print(f"[Worker {thread_id}] Final commit. Total treated: {count}.")

    cursor.close()
    conn.close()


def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]


def update_requests_table(db_params, batch_size=1000000):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    while True:
        # Select a batch of rows that still need updating
        cursor.execute("""
            SELECT r.id, utc.is_tracker
            FROM requests r
            JOIN url_tracking_classification utc
            ON SHA2(r.request_url, 256) = utc.url_hash
            WHERE r.is_tracker IS NULL
            LIMIT %s;
        """, (batch_size,))

        rows = cursor.fetchall()
        if not rows:
            break

        cursor.executemany(
            "UPDATE requests SET is_tracker = %s WHERE id = %s;",
            [(row['is_tracker'], row['id']) for row in rows]
        )
        conn.commit()
        print(f"Updated batch of {len(rows)} rows...")

    cursor.close()
    conn.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=10,
                        help="Max seconds allowed for should_block() per URL")
    args = parser.parse_args()
    timeout = args.timeout

    print('Getting new URLs')
    identify_new_urls(db_params)
    print('New URLs inserted')

    urls = get_non_treated_urls(db_params)
    print(f"Fetched {len(urls)} URLs to treat.")

    if urls:
        num_threads = 32
        url_chunks = split_list(urls, num_threads)


        print(f"Starting classification using {num_threads} workers...")
        with ProcessPoolExecutor(max_workers=num_threads, initializer=init_worker) as executor:
            futures = [executor.submit(process_chunk, chunk, db_params, timeout, i)
                       for i, chunk in enumerate(url_chunks)]
            for future in futures:
                future.result()

        print("All workers completed successfully.")

    print("Propagating tracking label to 'requests' table.")
    update_requests_table(db_params)
