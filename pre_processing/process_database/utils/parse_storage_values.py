import threading, pymysql, hashlib, json, re, math
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Dict, Any, Union
from urllib.parse import parse_qs, unquote_plus
from config import db_params


MYSQL_MAX_SAFE_DOUBLE = 1e308
MYSQL_MAX_SAFE_INT = 9007199254740991  # 2^53 - 1

def clean_value(val):
    # Remove leading/trailing spaces and wrapping quotes
    val = val.strip()
    if val.startswith('"') and val.endswith('"'):
        val = val[1:-1]
    return val



def clean_for_mysql_json(value):
    """Ensure the object is safe to insert into a MySQL JSON column."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value) or abs(value) > MYSQL_MAX_SAFE_DOUBLE:
            return str(value)
        return value

    
    elif isinstance(value, int):
        if abs(value) > MYSQL_MAX_SAFE_INT:
            return str(value)
        return value


    elif isinstance(value, dict):
        clean_dict = {}
        for k, v in value.items():
            try:
                k_encoded = k.encode('utf-8')
            except Exception:
                continue
            if len(k_encoded) > 512:
                continue
            clean_dict[k] = clean_for_mysql_json(v)
        return clean_dict


    elif isinstance(value, list):
        return [clean_for_mysql_json(v) for v in value]

    else:
        return value



def try_parse_json(val):
    try:
        return json.loads(val)
    except json.JSONDecodeError:
        try:
            # If it's a double-escaped string, unescape and parse again
            unescaped = val.encode().decode('unicode_escape')
            return json.loads(unescaped)
        except Exception:
            return None


def parse_storage_value(storage_key, storage_value):
    """
    Parses storage_value based on provided rules and returns a dict.
    """

    storage_value = storage_value.strip().strip('"').strip("'")


    # First, try parsing as JSON directly if it's truly JSON (starts with { or [)
    parsed_json = try_parse_json(storage_value)

    if isinstance(storage_value, str):
        parsed_json = try_parse_json(storage_value)
        if isinstance(parsed_json, dict):
            return parsed_json
        elif parsed_json is not None:
            return {storage_key: parsed_json}


    # Then, parse as semicolon-separated key=value pairs
    items = storage_value.split(";")
    kv_pairs = {}

    for item in items:
        item = item.strip()
        if not item:
            continue

        if "=" in item:
            key, val = item.split("=", 1)
            key = clean_value(key)
            val = clean_value(val)

            # Parse val as JSON if possible
            try:
                val_parsed = json.loads(val)
                kv_pairs[key] = val_parsed
                continue
            except (json.JSONDecodeError, TypeError):
                pass

            # If val contains '&' and '=', parse as query string
            if "&" in val and "=" in val:
                parsed_qs = parse_qs(val)
                parsed_qs = {
                    k: unquote_plus(v[0]) if len(v) == 1 else [unquote_plus(i) for i in v]
                    for k, v in parsed_qs.items()
                }
                kv_pairs[key] = parsed_qs
            else:
                kv_pairs[key] = val

        else:
            # Items without '=' treated as single value indicators (e.g. 'deleted')
            kv_pairs[item] = ""

    # Remove irrelevant cookie keys if only dealing with single value (Rule 2)
    irrelevant_keys = {"expires", "path", "domain", 'samesite', 'max-age', 'secure'}
    cleaned_kv_pairs = {k: v for k, v in kv_pairs.items() if k.lower() not in irrelevant_keys}

    if len(cleaned_kv_pairs) == 1:
        # Only one final key-value remains (Rule 2)
        only_key = next(iter(cleaned_kv_pairs))
        only_value = cleaned_kv_pairs[only_key]

        # If key has no value (e.g. deleted as key with ""), treat 'key' as value
        if only_value == "":
            return {storage_key: only_key}
        else:
            return {storage_key: only_value}

    # If multiple key-value pairs, return parsed dict (Rule 1)
    return cleaned_kv_pairs


def get_non_treated_storages(db_params, table_name):

    conn = pymysql.connect(**db_params)
    cursor = conn.cursor()

    query = f"SELECT id, storage_key, storage_value FROM {table_name} where parsed_value is null"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return rows



def process_chunk(storage_rows, db_params, table_name, thread_id):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor()


    total = len(storage_rows)
    count = 0
    updates = []

    for row in storage_rows:

        _id = row['id']
        key = row['storage_key']
        value = row['storage_value']

        query = f"UPDATE {table_name} set parsed_value = %s where id = %s;"

        parsed_value = parse_storage_value(key, value)
        parsed_value = clean_for_mysql_json(parsed_value)
        parsed_value = json.dumps(parsed_value)

        updates.append( (parsed_value, _id) )

        count += 1

        if count % 100 == 0:

            try:
                cursor.executemany(query, updates)
                conn.commit()
                print(f"[Thread {thread_id}] Treated {count} / {total} URLs.")

            except pymysql.err.OperationalError as e:

                if e.args[0] == 3140:
                    print(f"Skipped records due to invalid JSON numeric overflow:", _id)
                    conn.rollback()
                    pass
                else:
                    raise

            updates.clear()


    if updates:

        try:
            cursor.executemany(query, updates)
            conn.commit()
            print(f"[Thread {thread_id}] Final commit. Total treated: {count}.")

        except pymysql.err.OperationalError as e:

            if e.args[0] == 3140:
                print(f"Skipped records due to invalid JSON numeric overflow")
                conn.rollback()
                pass
            else:
                raise



    cursor.close()
    conn.close()

def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]



if __name__ == "__main__":


    for table in ['cookies', 'local_storage', 'session_storage']: 

        storage_rows = get_non_treated_storages(db_params, table)
        num_threads = 32
        storage_rows_chunks = split_list(storage_rows, num_threads)

        with ProcessPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i, chunk in enumerate(storage_rows_chunks):
                futures.append(
                    executor.submit(process_chunk, chunk, db_params, table, i)
                )

            for future in futures:
                future.result()

        print("All threads completed successfully for ", table)
