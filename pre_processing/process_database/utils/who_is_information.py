import json
import time
import random
import subprocess
import re

import pymysql
import tldextract
from config import db_params


def get_connection():
    return pymysql.connect(**db_params)


def ensure_whois_column():
    """
    Ensure crawl_sessions has a who_is column.
    Only creates it if it doesn't exist.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'crawl_sessions'
                  AND COLUMN_NAME = 'who_is'
                """
            )
            row = cur.fetchone()
            exists = int(row["cnt"]) > 0

            if not exists:
                print("[INFO] Adding who_is column to crawl_sessions...")
                cur.execute(
                    "ALTER TABLE crawl_sessions "
                    "ADD COLUMN who_is TEXT NULL"
                )
                conn.commit()
                print("[INFO] who_is column created.")
            else:
                print("[INFO] who_is column already exists, skipping ALTER.")
    finally:
        conn.close()


def get_pending_etlds():
    """
    Get DISTINCT etld where who_is is NULL.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT etld
                FROM crawl_sessions
                WHERE who_is IS NULL
                AND category = 'country_coded'
                ORDER BY etld
                """
            )
            rows = cur.fetchall()
            return [r["etld"] for r in rows]
    finally:
        conn.close()


def update_whois_for_etld(etld, whois_json_str):
    """
    Update all rows for this etld where who_is is still NULL.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crawl_sessions
                   SET who_is = %s
                 WHERE etld = %s
                   AND who_is IS NULL
                """,
                (whois_json_str, etld),
            )
        conn.commit()
    finally:
        conn.close()


# ------------- WHOIS helpers ------------- #

def canonical_domain(hostname: str) -> str:
    """
    Normalize to registrable domain (eTLD+1).
    e.g. 'aamen.doh.gov.ae' -> 'doh.gov.ae'
         'www.example.com'   -> 'example.com'
    """
    ext = tldextract.extract(hostname)
    return ext.top_domain_under_public_suffix or hostname


def fetch_whois_raw(domain: str) -> str | None:
    """
    Call the system `whois` command and return the raw text,
    or None on failure / rate limit.
    """
    try:
        result = subprocess.run(
            ["whois", domain],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception as e:
        print(f"[WARN] whois command failed for {domain}: {e}")
        return None

    raw = (result.stdout or "").strip()

    if result.returncode != 0 or not raw:
        print(f"[WARN] whois returned no data for {domain}")
        return None

    # detect rate-limiting / blacklist messages and treat as no data
    upper_raw = raw.upper()
    if (
        "BLACKLISTED" in upper_raw
        or "EXCEEDED THE QUERY LIMIT" in upper_raw
        or "QUERY LIMIT EXCEEDED" in upper_raw
        or "TOO MANY REQUESTS" in upper_raw
    ):
        print(f"[WARN] WHOIS rate limit / blacklist for {domain}, skipping.")
        return None

    return raw


def parse_owner_info(raw: str) -> dict:
    """
    Best-effort extraction of ownership-ish info:
    registrar, registrant org/name/email.
    Works OK for many gTLDs; for some ccTLDs you'll just get very sparse info.
    """

    def match_first(pattern: str):
        m = re.search(pattern, raw, flags=re.IGNORECASE)
        return m.group(1).strip() if m else None

    # Registrar (may not exist or be hidden on some ccTLDs)
    registrar = (
        match_first(r"Registrar:\s*(.+)")
        or match_first(r"Sponsoring Registrar:\s*(.+)")
        or match_first(r"Registrar Name:\s*(.+)")
    )

    # Registrant org/name/email: lots of variants, we just cover common ones
    registrant_org = (
        match_first(r"Registrant Organization:\s*(.+)")
        or match_first(r"Registrant Organisation:\s*(.+)")
        or match_first(r"OrgName:\s*(.+)")
        or match_first(r"Organisation:\s*(.+)")
    )

    registrant_name = (
        match_first(r"Registrant Name:\s*(.+)")
        or match_first(r"Admin Name:\s*(.+)")
        or match_first(r"Owner Name:\s*(.+)")
    )

    registrant_email = (
        match_first(r"Registrant Email:\s*(.+)")
        or match_first(r"Admin Email:\s*(.+)")
        or match_first(r"Owner Email:\s*(.+)")
        or match_first(r"Email:\s*([^\s]+@[^\s]+)")
    )

    return {
        "registrar": registrar,
        "registrant_org": registrant_org,
        "registrant_name": registrant_name,
        "registrant_email": registrant_email,
    }


# ------------- Main logic ------------- #

def main():
    ensure_whois_column()

    etlds = get_pending_etlds()
    print(f"[INFO] Found {len(etlds)} etld(s) with who_is IS NULL")

    if not etlds:
        return

    # per canonical domain cache so we don't hammer WHOIS for same owner
    owner_cache: dict[str, dict] = {}

    min_sleep = 5.0
    max_sleep = 7.5

    for i, etld in enumerate(etlds, 1):
        print(f"[INFO] ({i}/{len(etlds)}) Processing etld={etld}...")
        whois_domain = canonical_domain(etld)

        # If we already got info for this canonical domain, just reuse
        if whois_domain in owner_cache:
            data = owner_cache[whois_domain].copy()
            data["domain_queried"] = etld
            whois_json_str = json.dumps(data, separators=(",", ":"))
            update_whois_for_etld(etld, whois_json_str)
            continue

        # Otherwise, query WHOIS once for this canonical domain
        raw = fetch_whois_raw(whois_domain)
        if not raw:
            print(f"[WARN] No WHOIS data for {whois_domain}, leaving who_is NULL.")
            # don't sleep extra here if nothing came back? still be gentle
            time.sleep(random.uniform(min_sleep, max_sleep))
            continue

        parsed = parse_owner_info(raw)

        # Determine if we got anything useful
        useful = any(
            [
                parsed["registrar"],
                parsed["registrant_org"],
                parsed["registrant_name"],
                parsed["registrant_email"],
            ]
        )

        distilled = {
            "source": "whois",
            "domain_queried": etld,       # original host
            "whois_domain": whois_domain, # canonical domain (eTLD+1)
            "registrar": parsed["registrar"],
            "registrant": {
                "org": parsed["registrant_org"],
                "name": parsed["registrant_name"],
                "email": parsed["registrant_email"],
            },
            "parsed_ok": useful,
        }

        # cache per whois_domain
        owner_cache[whois_domain] = distilled.copy()

        if useful:
            whois_json_str = json.dumps(distilled, separators=(",", ":"))
            update_whois_for_etld(etld, whois_json_str)
            print(
                f"[INFO] Stored ownership info for {etld} "
                f"(whois_domain={whois_domain})"
            )
        else:
            print(
                f"[WARN] WHOIS for {whois_domain} had no clear owner fields; "
                f"leaving who_is NULL."
            )

        time.sleep(random.uniform(min_sleep, max_sleep))


if __name__ == "__main__":
    main()
