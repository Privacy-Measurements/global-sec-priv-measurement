import json
import pymysql
from utils.config import db_params


def get_connection():
    return pymysql.connect(
        **db_params,
        autocommit=False
    )


def ensure_parent_id_column(conn):
    """
    Check if requests.parent_id exists; if not, create it.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS cnt
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'requests'
              AND COLUMN_NAME = 'parent_id'
        """)
        exists = cur.fetchone()["cnt"] > 0

        if not exists:
            print("[INFO] Adding column requests.parent_id (VARCHAR(50))")
            cur.execute("""
                ALTER TABLE requests
                ADD COLUMN parent_id VARCHAR(50) NULL
            """)
            conn.commit()
        else:
            print("[INFO] Column requests.parent_id already exists")


def fetch_exactly_one(cur, sql, params):
    cur.execute(sql, params)
    rows = cur.fetchall()
        
    if len(rows) != 1:
        return None

    return rows[0]



def populate_parent_id(filename, country, category, batch_size=100):
    """
    Update requests.parent_id using data from JSON file.
    """
    print(f"[START] {filename} | {country} | {category}")

    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{filename}: JSON root must be a list")

    conn = get_connection()
    updated = 0
    processed = 0
    no_corresponding_session = 0
    no_corresponding_request = 0

    try:
        with conn.cursor() as cur:
            for i, entry in enumerate(data):
                processed += 1

                site = entry["site"]
                page_url = entry["url"]
                request_id = entry["request_id"]
                request_url = entry["request_url"]
                result_hash = entry["result_hash"]
                result_size = entry["result_size"]
                result_status = entry["result_status"]
                frame_id = entry["frame_id"]
                script_id = entry["script_id"]


                # ----------------------------
                # Find crawl_session
                # ----------------------------
                session_sql = """
                    SELECT id
                    FROM crawl_sessions
                    WHERE category = %s
                      AND location = %s
                      AND etld = %s
                      AND url = %s
                """

                session_row = fetch_exactly_one(
                    cur,
                    session_sql,
                    (category, country, site, page_url)
                )

                if not session_row:
                    print(f"[WARN] No session found for entry {i}\n{entry}")
                    no_corresponding_session+=1
                    continue

                session_id = session_row["id"]

                # ----------------------------
                # Find request
                # ----------------------------

                #   AND result_hash = %s
                #   AND result_size = %s
                #   AND result_status = %s
                #   AND request_url = %s

                request_sql = """
                    SELECT id
                    FROM requests
                    WHERE session_id = %s
                      AND request_id = %s
                      AND frame_id = %s
                """

                request_row = fetch_exactly_one(
                    cur,
                    request_sql,
                    (
                        session_id,
                        request_id,
                       # request_url,
                      #  result_hash,
                      #  result_size,
                     #   result_status,
                        frame_id,
                    )
                )


                if not request_row:
                    print(f"[WARN] No request found for entry {i}\n{entry}")
                    no_corresponding_request+=1
                    continue

                request_db_id = request_row["id"]

                # ----------------------------
                # Update parent_id
                # ----------------------------
                cur.execute(
                    """
                    UPDATE requests
                    SET parent_id = %s
                    WHERE id = %s
                    """,
                    (script_id, request_db_id),
                )
                updated += cur.rowcount
                if processed % batch_size == 0:
                    conn.commit()
                    print(f"[INFO] processed={processed}, updated={updated}")

        conn.commit()

        print(f"[DONE] {filename}: processed={processed}, updated={updated}")
        print('no corresponding session:', no_corresponding_session)
        print('no corresponding request:', no_corresponding_request)
        return updated, processed

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    conn = get_connection()
    ensure_parent_id_column(conn)
    conn.close()

    populate_parent_id(
        filename="../../data/files_to_analyze/REQUESTS_TO_SCRIPTS_DE_GLOBAL.json",
        country="GERMANY",
        category="global",
    )
