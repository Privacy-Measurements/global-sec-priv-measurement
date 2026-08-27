import json
import pymysql
import re
from datetime import datetime
from difflib import SequenceMatcher
from concurrent.futures import ProcessPoolExecutor, as_completed
from config import db_params
from wordfreq import top_n_list

ENGLISH_WORDS = set(w.lower() for w in top_n_list("en", n=500_000))

num_threads = 16

# For access_id X, use access_id ACCESS_MAP[X] as the reference baseline
ACCESS_MAP = {'1': '2', '2': '3', '3': '1'}


def get_reference_sessions(db_params, base_sessions):
    """
    For each session in base_sessions, fetch the reference session used for UID filtering.
    The reference access_id is determined by ACCESS_MAP: 1→2, 2→3, 3→1.
    Keyed by (etld, location, ref_access_id).
    """
    triplets = set()
    for s in base_sessions:
        ref_access_id = ACCESS_MAP[str(s["access_id"])]
        triplets.add((s["etld"], s["location"], ref_access_id))

    if not triplets:
        return []

    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    placeholders = ", ".join(["(%s, %s, %s)"] * len(triplets))
    query = f"""
        SELECT id, etld, url, location, access_id
        FROM crawl_sessions
        WHERE (etld, location, access_id) IN ({placeholders})
    """

    params = []
    for t in triplets:
        params.extend(t)

    cursor.execute(query, params)
    reference_sessions = cursor.fetchall()

    cursor.close()
    conn.close()

    return reference_sessions


def get_non_treated_sessions(db_params):

    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    query = """
    SELECT id, etld, url, category, location, access_id
    FROM crawl_sessions
    WHERE (user_identifiers IS NULL OR is_user_identifiers IS NULL)
    AND access_id IN ('1', '2', '3')
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows
    
def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def flatten_dict(d, parent_key='', sep='..'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def is_probable_url(s):
    return isinstance(s, str) and (
        s.startswith("http://") or s.startswith("https://") or "www." in s
    )


def is_probable_word_string(s):

    if not isinstance(s, str):
        return False

    WORD_RE = re.compile(r"[a-zA-Z]{4,}")  

    s = s.lower()

    for token in WORD_RE.findall(s):
        if token in ENGLISH_WORDS:
            return True

    return False


    return bool(re.match(r"^[a-zA-Z\s]+$", s))




def is_probable_timestamp(val):
    try:
        val = int(val)
        dt = datetime.utcfromtimestamp(val / 1000 if val > 1e10 else val)
        return datetime(2025, 4, 1) <= dt <= datetime(2025, 12, 31)
    except Exception:
        return False


def is_identifier(value):

    if not isinstance(value, str):
        value = str(value)
    value = value.strip()

    if not (8 <= len(value) <= 100):
        return False
    if is_probable_url(value):
        return False
    if is_probable_word_string(value):
        return False
    if is_probable_timestamp(value):
        return False
    return True




def process_storage_table(session, table_name, conn, cursor):
    session_id = session['id']

    if table_name == 'cookies':
        query = f"SELECT id, parsed_value, event_type, caller_id, caller_type, caller_hash, script_type, caller_url FROM {table_name} WHERE session_id = %s"
    else:
        query = f"SELECT id, parsed_value, caller_id, caller_type, caller_hash, caller_url FROM {table_name} WHERE session_id = %s"


    cursor.execute(query, (session_id,))
    rows = cursor.fetchall()

    identifier_values_only = []
    seen_jsons = set()

    to_update_true = []
    to_update_false = []

    for row in rows:
        parsed_value = row["parsed_value"]

        if parsed_value is None:
            # Skip rows with NULL parsed_value, mark as non-identifier
            to_update_false.append(row['id'])
            continue

        if isinstance(parsed_value, str):
            try:
                parsed_value = json.loads(parsed_value)
            except Exception:
                raise ValueError(
                    f"[process_storage_table] Invalid JSON in {table_name} for row id={row['id']} (session_id={session_id}): {e}"
                )

        parsed_value = flatten_dict(parsed_value)
        has_identifier = False

        for key, val in parsed_value.items():

            if not is_identifier(val):
                continue

            has_identifier = True
            
            data = {
                'key': key,
                'val': val,
                'caller_id': row['caller_id'],
                'caller_type': row['caller_type'],
                'caller_hash': row['caller_hash'],
                'caller_url': row['caller_url']
            }

            if 'event_type' in row:
                data['event_type'] = row['event_type']

            if 'script_type' in row:
                data['script_type'] = row['script_type']

            data_json = json.dumps(data, sort_keys=True)
            if data_json in seen_jsons:
                continue

            seen_jsons.add(data_json)
            identifier_values_only.append(data)

        if has_identifier:
            to_update_true.append(row['id'])
        else:
            to_update_false.append(row['id'])


    if to_update_true:

        BATCH_SIZE = 1000
        for i in range(0, len(to_update_true), BATCH_SIZE):
            batch_ids = to_update_true[i:i+BATCH_SIZE]
            query = f"UPDATE {table_name} SET is_identifier = TRUE WHERE id IN ({','.join(['%s'] * len(batch_ids))})"
            cursor.execute(query, batch_ids)

            conn.commit()


    if to_update_false:

        BATCH_SIZE = 1000
        for i in range(0, len(to_update_false), BATCH_SIZE):
            batch_ids = to_update_false[i:i+BATCH_SIZE]
            query = f"UPDATE {table_name} SET is_identifier = FALSE WHERE id IN ({','.join(['%s'] * len(batch_ids))})"
            cursor.execute(query, batch_ids)

            conn.commit()

    return identifier_values_only


def process_reference_chunk(sessions_chunk, db_params, thread_id):

    print('Starting with thread:', thread_id)

    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    results = []
    total = len(sessions_chunk)

    for count, session in enumerate(sessions_chunk, 1):
        etld = session["etld"]
        location = session["location"]
        access_id = str(session["access_id"])

        cookies = process_storage_table(session, "cookies", conn, cursor)
        session_storage = process_storage_table(session, "session_storage", conn, cursor)
        local_storage = process_storage_table(session, "local_storage", conn, cursor)

        results.append(((etld, location, access_id), {
            "session_id": session["id"],
            "cookies": cookies,
            "session_storage": session_storage,
            "local_storage": local_storage
        }))

        if count % 50 == 0:
            print(f"[Reference Thread {thread_id}] Processed {count}/{total} sessions ")


    cursor.close()
    conn.close()
    return results

def process_reference_sessions(sessions, db_params):
    """
    Process reference sessions in parallel and return a dict keyed by
    (etld, location, access_id) → list of extracted identifier dicts.
    """
    validations_by_ref_key = {}

    sessions_chunks = split_list(sessions, num_threads)
    with ProcessPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(process_reference_chunk, chunk, db_params, i)
            for i, chunk in enumerate(sessions_chunks)
        ]

        for future in as_completed(futures):
            chunk_results = future.result()
            for ref_key, result in chunk_results:
                validations_by_ref_key.setdefault(ref_key, []).append(result)

    return validations_by_ref_key


def similarity_score(a, b):
    return SequenceMatcher(None, str(a), str(b), autojunk=False).ratio()


def compare_identifiers(current_identifiers, validation_identifiers):
    filtered_identifiers = []
    for cid in current_identifiers:
        keep = True
        for vid in validation_identifiers:
            if cid["key"] == vid["key"]:
                score = similarity_score(cid["val"], vid["val"])
                if score > 0.33:
                    keep = False
                    break
        if keep:
            filtered_identifiers.append(cid)
    return filtered_identifiers


def process_chunk(sessions, db_params, thread_id, validations_by_ref_key):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    total = len(sessions)
    count = 0
    updates = []

    for session in sessions:
        etld = session["etld"]
        ref_access_id = ACCESS_MAP[str(session["access_id"])]
        ref_key = (etld, session["location"], ref_access_id)
        reference_sessions = validations_by_ref_key.get(ref_key, [])

        uids_in_cookies = process_storage_table(session, 'cookies', conn, cursor)
        uids_in_session_storage = process_storage_table(session, 'session_storage', conn, cursor)
        uids_in_local_storage = process_storage_table(session, 'local_storage', conn, cursor)

        for reference in reference_sessions:
            uids_in_cookies = compare_identifiers(uids_in_cookies, reference["cookies"])
            uids_in_session_storage = compare_identifiers(uids_in_session_storage, reference["session_storage"])
            uids_in_local_storage = compare_identifiers(uids_in_local_storage, reference["local_storage"])

        if len(uids_in_cookies) + len(uids_in_session_storage) + len(uids_in_local_storage) > 0:
            is_user_identifiers = True
        else:
            is_user_identifiers = False

        summary = {
            'cookies_storage_identifiers': uids_in_cookies,
            'session_storage_identifiers': uids_in_session_storage,
            'local_storage_identifiers': uids_in_local_storage
        }

        updates.append((is_user_identifiers, json.dumps(summary), session['id']))
        count += 1

        if count % 100 == 0:
            cursor.executemany(
                "UPDATE crawl_sessions SET is_user_identifiers = %s, user_identifiers = %s WHERE id = %s;",
                updates
            )
            conn.commit()
            print(f"[Thread {thread_id}] Treated {count} / {total} sessions.")
            updates.clear()



    if updates:
        cursor.executemany(
            "UPDATE crawl_sessions SET is_user_identifiers = %s, user_identifiers = %s WHERE id = %s;",
            updates
        )
        conn.commit()
        print(f"[Thread {thread_id}] Final commit. Total treated: {count}.")


    cursor.close()
    conn.close()





if __name__ == "__main__":

    print("Fetching normal sessions...")
    normal_sessions = get_non_treated_sessions(db_params)
    print(f"Found {len(normal_sessions)} normal sessions.")

    if len(normal_sessions) == 0:
        print("No sessions to process. Exiting.")
        exit(0)


    print("Fetching reference sessions...")
    reference_sessions = get_reference_sessions(db_params, normal_sessions)
    print(f"Reference sessions found: {len(reference_sessions)}")


    print("Processing reference sessions...")
    validations_by_ref_key = process_reference_sessions(reference_sessions, db_params)


    sessions_chunks = split_list(normal_sessions, num_threads)

    with ProcessPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i, chunk in enumerate(sessions_chunks):
            futures.append(
                executor.submit(process_chunk, chunk, db_params, i, validations_by_ref_key)
            )

        for future in futures:
            future.result()

    print("Processing complete.")
