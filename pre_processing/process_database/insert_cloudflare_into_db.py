import argparse
import json
import sys
import pymysql
import os
from utils.config import db_params



def ensure_is_cloudflare_column_exists(conn):
    """Check if is_cloudflare column exists; if not, create it with NULL default."""
    with conn.cursor() as cur:
        cur.execute("SHOW COLUMNS FROM crawl_sessions LIKE 'is_cloudflare'")
        result = cur.fetchone()
        if result:
            print("Column is_cloudflare already exists.")
            return

        print("Column is_cloudflare does not exist. Creating it...")
        cur.execute(
            """
            ALTER TABLE crawl_sessions
            ADD COLUMN is_cloudflare TINYINT(1) NULL DEFAULT NULL
            """
        )
    conn.commit()
    print("Column is_cloudflare created (initialized to NULL for all rows).")


def update_rows_from_file(conn, filename):
    """Read JSON file and update crawl_sessions for each element."""
    try:
        base_path = "../../data/files_to_analyze"
        file_path = os.path.join(base_path, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading/parsing JSON file {filename}: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print("JSON root must be a list of objects.", file=sys.stderr)
        sys.exit(1)

    with conn.cursor() as cur:
        # Explicit transaction so we can roll back on first error
        conn.begin()

        for idx, item in enumerate(data, start=1):
            try:
                country = item["country"]   
                category = item["category"]
                etld = item["etld"]
                url = item["url"]
                cloudflare = item["cloudflare"]
            except KeyError as e:
                msg = f"Missing key {e} in item #{idx}: {item}"
                print(msg, file=sys.stderr)
                conn.rollback()
                sys.exit(1)

            if url == 'unknown' or url == '':
                continue

            # Convert bool to int for MySQL TINYINT(1)
            if isinstance(cloudflare, bool):
                cloudflare_value = int(cloudflare)
            elif cloudflare is None:
                cloudflare_value = None
            else:
                msg = (
                    f"Invalid 'cloudflare' value in item #{idx}: {cloudflare!r} "
                    f"(expected bool or null)"
                )
                print(msg, file=sys.stderr)
                conn.rollback()
                sys.exit(1)

            # NOTE: DB uses 'location', JSON uses 'country'
            sql = """
                UPDATE crawl_sessions
                SET is_cloudflare = %s
                WHERE etld = %s
                  AND url = %s
                  AND location = %s
                  AND category = %s
            """
            params = (cloudflare_value, etld, url, country, category)

            try:
                cur.execute(sql, params)
            except Exception as e:
                msg = (
                    f"Error executing UPDATE for item #{idx} "
                    f"(etld={etld}, url={url}, country={country}, category={category}): {e}"
                )
                print(msg, file=sys.stderr)
                conn.rollback()
                sys.exit(1)

            if cur.rowcount == 0:
                msg = (
                    f"No matching row found for item #{idx}: "
                    f"(etld={etld}, url={url}, location={country}, category={category})"
                )
                print(msg, file=sys.stderr)
               # conn.rollback()
               # sys.exit(1)
            elif cur.rowcount > 1:
                msg = (
                    f"UPDATE affected {cur.rowcount} rows for item #{idx} "
                    f"(expected exactly 1). "
                    f"(etld={etld}, url={url}, location={country}, category={category})"
                )
                print(msg, file=sys.stderr)
                #conn.rollback()
                #sys.exit(1)

        conn.commit()
        print(f"Successfully updated {len(data)} rows.")


def main():
    parser = argparse.ArgumentParser(
        description="Update crawl_sessions.is_cloudflare from a JSON file."
    )
    parser.add_argument(
        "filename",
        help="Path to JSON file containing a list of objects with keys "
             "country, category, etld, url, cloudflare",
    )
    args = parser.parse_args()

    try:
        conn = pymysql.connect(**db_params)
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        ensure_is_cloudflare_column_exists(conn)
        update_rows_from_file(conn, args.filename)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
