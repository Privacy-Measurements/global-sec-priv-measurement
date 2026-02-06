
import threading, pymysql, hashlib, json, re, math
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Dict, Any, Union
from urllib.parse import parse_qs, unquote_plus
from config import db_params




TABLE_TO_JS_CALL = {
    'local_storage': 'Window.localStorage.get',
    'session_storage': 'Window.sessionStorage.get'
}

ignore_keys = {"Symbol(Symbol.toStringTag)", "length"}

MAX_STORAGE_VALUE_LENGTH = 65000  # safe limit for TEXT columns


def batch_insert(cursor, table_name, all_results, thread_id):

    if not all_results:
        return

    columns = "(session_id, storage_key, storage_value, caller_id, caller_type, caller_hash, caller_url)"
    placeholders = "(%s, %s, %s, %s, %s, %s, %s)"

    data = []

    for r in all_results:
        value_str = str(r["storage_value"])
        value_bytes = value_str.encode("utf-8")

        if len(value_bytes) > MAX_STORAGE_VALUE_LENGTH: # skip oversized values
            continue  

        data.append((
            r["session_id"],
            r["storage_key"],
            value_str,
            r["caller_id"],
            r["caller_type"],
            r["caller_hash"],
            r["caller_url"],
        ))

    if not data:
        return

    cursor.executemany(f"INSERT INTO {table_name} {columns} VALUES {placeholders}", data)




def get_non_treated_sessions(db_params, table):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor()

    query = f"SELECT id FROM crawl_sessions cs WHERE id NOT IN (SELECT DISTINCT session_id FROM {table})"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [row["id"] for row in rows]


def get_js_calls_for_crawl_session(cursor, session_id, call_method):

    query = f"""
        SELECT session_id, caller_id, caller_type, caller_hash, caller_url, call_result
        FROM js_calls
        WHERE call_method = %s AND session_id = %s
    """

    cursor.execute(query, (call_method, session_id))
    rows = cursor.fetchall()

    return rows


def parse_call_result(call_result):
    parsed_result = {}
    if isinstance(call_result, str):
        try:
            parsed_result = json.loads(call_result)
        except Exception:
            try:
                parsed_result = eval(call_result)
            except Exception:
                pass
    elif isinstance(call_result, dict):
        parsed_result = call_result

    return parsed_result


def process_crawl_session(cursor, session_id, table):

    js_calls = get_js_calls_for_crawl_session(cursor, session_id, TABLE_TO_JS_CALL[table])
    results = []

    for row in js_calls:
        call_result = row.get("call_result")

        
        parsed_result = parse_call_result(call_result)

        if not parsed_result:
            continue


        # Remove ignored keys
        for key in ignore_keys:
            parsed_result.pop(key, None)

        for storage_key, storage_value in parsed_result.items():
            results.append({
                "session_id": row["session_id"],
                "storage_key": storage_key,
                "storage_value": storage_value,
                "caller_id": row["caller_id"],
                "caller_hash": row["caller_hash"],
                "caller_type": row["caller_type"],
                "caller_url": row["caller_url"],
            })


    results = list({json.dumps(r, sort_keys=True) for r in results})
    results = [json.loads(r) for r in results]

    return results


def process_chunk(crawl_sessions, db_params, table_name, thread_id):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    total_sessions = len(crawl_sessions)

    for idx, session_id in enumerate(crawl_sessions, start=1):

        results = process_crawl_session(cursor, session_id, table_name)

        if not results:
            continue 

        batch_insert(cursor, table_name, results, thread_id)
        conn.commit()

        print(f"[Thread {thread_id}] Progress: {idx}/{total_sessions} sessions processed")

    cursor.close()
    conn.close()


def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]



if __name__ == "__main__":


    for table in ['local_storage', 'session_storage']:

        crawl_sessions = get_non_treated_sessions(db_params, table)

        num_threads = 16

        crawl_sessions_chunks = split_list(crawl_sessions, num_threads)

        with ProcessPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i, chunk in enumerate(crawl_sessions_chunks):
                futures.append(
                    executor.submit(process_chunk, chunk, db_params, table, i)
                )

            for future in futures:
                future.result()

        print("All threads completed successfully for ", table)
