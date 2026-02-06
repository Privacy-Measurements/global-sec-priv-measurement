import json
import pymysql
from utils.config import db_params


def get_connection():
    return pymysql.connect(
        **db_params,
        autocommit=False
    )


def ensure_html_elements_table(conn):
    """
    Create table ensure_html_elements_table if it doesn't exist.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS html_elements (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

                session_id INT NOT NULL,
                element_id VARCHAR(50) NOT NULL,
                tag VARCHAR(20) NOT NULL,
                src TEXT NULL,
                attrs_json LONGTEXT NULL,

                parent_id VARCHAR(50) NULL,
                parent_type VARCHAR(50) NULL,

                UNIQUE KEY uniq_session_element (session_id, element_id),
                KEY idx_session_id (session_id),
                KEY idx_tag (tag),

                CONSTRAINT fk_html_elements_session
                    FOREIGN KEY (session_id) REFERENCES crawl_sessions(id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

        """)
        conn.commit()


def fetch_exactly_one(cur, sql, params):
    cur.execute(sql, params)
    rows = cur.fetchall()
    if len(rows) != 1:
        return None
    return rows[0]


def populate_html_elements(filename, country, category, batch_size=500):
    print(f"[START] {filename} | {country} | {category}")

    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{filename}: JSON root must be a list")

    conn = get_connection()

    processed = inserted = no_session = 0

    session_sql = """
        SELECT id
        FROM crawl_sessions
        WHERE category = %s
          AND location = %s
          AND etld = %s
          AND url = %s
    """

    insert_sql = """
        INSERT IGNORE INTO html_elements
            (session_id, element_id, tag, src, attrs_json, parent_id, parent_type)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
    """

    try:
        with conn.cursor() as cur:
            for i, entry in enumerate(data):
                processed += 1

                site = entry["site"]
                page_url = entry["url"]

                element_id = entry["id"]
                tag = entry["tag"]

                src = entry["src"]
                attrs = entry["attrs"]
                parent_id = entry["parent_id"]
                parent_type = entry["parent_type"]

                if not isinstance(attrs, dict):
                    attrs = {}

                attrs_json = json.dumps(attrs, ensure_ascii=False)

                session_row = fetch_exactly_one(
                    cur,
                    session_sql,
                    (category, country, site, page_url),
                )

                if not session_row:
                    no_session += 1
                    continue

                session_id = session_row["id"]

                cur.execute(
                    insert_sql,
                    (
                        session_id,
                        element_id,
                        tag,
                        src,
                        attrs_json,
                        parent_id,
                        parent_type
                    )
                )
                inserted += cur.rowcount 

                if processed % batch_size == 0:
                    conn.commit()
                    print(
                        f"[INFO] processed={processed} inserted={inserted} "
                        f"no_session={no_session}"
                    )

        conn.commit()

        print(f"[DONE] processed={processed} inserted={inserted}")
        print(f"no corresponding session: {no_session}")

        return processed, inserted

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    conn = get_connection()
    ensure_html_elements_table(conn)
    conn.close()

    populate_html_elements(
        filename="../../data/files_to_analyze/HTML_ELEMENTS_GLOBAL_DE.json",
        country="GERMANY",
        category="global",
        batch_size=500,
    )

#    populate_html_elements(
#        filename="../../data/files_to_analyze/HTML_ELEMENTS_GLOBAL_DZ.json",
#        country="ALGERIA",
#        category="global",
#        batch_size=500,
#    )

 #   populate_html_elements(
 #       filename="../../data/files_to_analyze/HTML_ELEMENTS_GLOBAL_IN.json",
 #       country="INDIA",
 #       category="global",
 #       batch_size=500,
#    )

#    populate_html_elements(
#        filename="../../data/files_to_analyze/HTML_ELEMENTS_GLOBAL_US.json",
#        country="USA",
#        category="global",
#        batch_size=500,
#    )
