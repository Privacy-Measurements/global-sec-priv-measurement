import asyncio, base64, csv, json, re, sqlite3, threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[4]
INPUT_PATH = REPO_ROOT / "site_catalogs_construction" / "catalogs" / "D2_to_global"
OUTPUT_DIR = REPO_ROOT / "data" / "favicons"
JS_CRAWLER_PATH = Path(__file__).resolve().parent / "crawl_favicons.js"
CONCURRENCY = 1
TIMEOUT_SEC = 120
SETTLE_MS = 10000

STATUS_PENDING = 0
STATUS_IN_PROGRESS = 1
STATUS_SUCCESS = 2
STATUS_FAILED = -1


def _is_browser_closed(info) -> bool:
    """Return True if an exception or error string indicates the browser/context was closed."""
    s = str(info).lower()
    return (
        "targetclosed" in s
        or "browser has been closed" in s
        or "target page, context or browser" in s
        or "connection closed" in s
    )

MIME_EXT = {
    "image/x-icon": ".ico",
    "image/vnd.microsoft.icon": ".ico",
    "image/ico": ".ico",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/avif": ".avif",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_site(raw: str):
    s = (raw or "").strip().strip('"').strip("'")
    if not s:
        return None
    if "://" not in s:
        s = f"https://{s}"
    p = urlparse(s)
    host = (p.netloc or p.path).split("/")[0].split(":")[0].lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host if "." in host else None


def read_sites(csv_path: Path):
    sites = set()
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return []

    header_names = {"site", "domain", "url", "host", "website"}
    header = [c.strip().lower() for c in rows[0]] if rows[0] else []
    has_header = any(c in header_names for c in header)
    start = 1 if has_header else 0

    site_col_idx = 0
    if has_header:
        for name in ("site", "domain", "host", "website", "url"):
            if name in header:
                site_col_idx = header.index(name)
                break

    for r in rows[start:]:
        if not r or site_col_idx >= len(r):
            continue
        s = normalize_site(r[site_col_idx])
        if s:
            sites.add(s)
    return sorted(sites)


def ext_from_ct(ct: str, url: str):
    ct = (ct or "").split(";")[0].strip().lower()
    if ct in MIME_EXT:
        return MIME_EXT[ct]
    m = re.search(r"\.(ico|png|jpe?g|webp|svg|gif|bmp|avif)$", urlparse(url).path.lower())
    if not m:
        return ".bin"
    e = m.group(1)
    return ".jpg" if e == "jpeg" else f".{e}"


class CrawlDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def close(self):
        self.conn.close()

    def _init_schema(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site TEXT NOT NULL UNIQUE,
                status INTEGER NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                attempted_url TEXT,
                homepage_url TEXT,
                favicon_url TEXT,
                favicon_file TEXT,
                favicon_blob BLOB,
                content_type TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sites_status ON sites(status);")

    def seed_sites(self, sites: list[str]):
        now = utc_now()
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE;")
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO sites (site, status, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                [(s, STATUS_PENDING, now, now) for s in sites],
            )
            self.conn.execute("COMMIT;")

    def requeue_in_progress(self):
        now = utc_now()
        with self.lock:
            self.conn.execute(
                """
                UPDATE sites
                SET status = ?, updated_at = ?
                WHERE status = ?
                """,
                (STATUS_PENDING, now, STATUS_IN_PROGRESS),
            )

    def claim_next(self):
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE;")
            row = self.conn.execute(
                """
                SELECT id, site
                FROM sites
                WHERE status = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (STATUS_PENDING,),
            ).fetchone()

            if row is None:
                self.conn.execute("COMMIT;")
                return None

            now = utc_now()
            cur = self.conn.execute(
                """
                UPDATE sites
                SET status = ?, attempt_count = attempt_count + 1, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (STATUS_IN_PROGRESS, now, row["id"], STATUS_PENDING),
            )
            self.conn.execute("COMMIT;")

            if cur.rowcount != 1:
                return None
            return int(row["id"]), str(row["site"])

    def mark_success(self, site_id: int, attempted_url: str, homepage_url: str, favicon_url: str, favicon_file: str, favicon_blob: bytes, content_type: str):
        now = utc_now()
        with self.lock:
            self.conn.execute(
                """
                UPDATE sites
                SET status = ?,
                    attempted_url = ?,
                    homepage_url = ?,
                    favicon_url = ?,
                    favicon_file = ?,
                    favicon_blob = ?,
                    content_type = ?,
                    error = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (STATUS_SUCCESS, attempted_url, homepage_url, favicon_url, favicon_file, sqlite3.Binary(favicon_blob), content_type, now, site_id),
            )

    def mark_failed(self, site_id: int, attempted_url: str, homepage_url: str, favicon_url: str, error: str):
        now = utc_now()
        with self.lock:
            self.conn.execute(
                """
                UPDATE sites
                SET status = ?,
                    attempted_url = ?,
                    homepage_url = ?,
                    favicon_url = ?,
                    error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (STATUS_FAILED, attempted_url, homepage_url, favicon_url, error[:2000], now, site_id),
            )

    def stats(self):
        rows = self.conn.execute("SELECT status, COUNT(*) AS n FROM sites GROUP BY status").fetchall()
        out = {STATUS_PENDING: 0, STATUS_IN_PROGRESS: 0, STATUS_SUCCESS: 0, STATUS_FAILED: 0}
        for r in rows:
            out[int(r["status"])] = int(r["n"])
        return out

    def export_csv(self, out_csv: Path):
        rows = self.conn.execute(
            """
            SELECT site, status, attempt_count, attempted_url, homepage_url,
                   favicon_url, favicon_file, content_type, error, updated_at
            FROM sites
            ORDER BY id
            """
        ).fetchall()
        fields = ["site", "status", "attempt_count", "attempted_url", "homepage_url", "favicon_url", "favicon_file", "content_type", "error", "updated_at"]
        with out_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r[k] for k in fields})

    def export_jsonl(self, out_jsonl: Path):
        rows = self.conn.execute(
            """
            SELECT site, status, attempt_count, attempted_url, homepage_url,
                   favicon_url, favicon_file, content_type, error, updated_at
            FROM sites
            ORDER BY id
            """
        ).fetchall()
        with out_jsonl.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(dict(r), ensure_ascii=False) + "\n")


async def run_js_crawler(site: str, timeout_sec: int, settle_ms: int, node_bin: str, js_crawler_path: Path):
    cmd = [
        node_bin,
        str(js_crawler_path),
        "--site",
        site,
        "--timeout-sec",
        str(timeout_sec),
        "--settle-ms",
        str(settle_ms),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    out = (stdout or b"").decode("utf-8", errors="ignore").strip()
    err = (stderr or b"").decode("utf-8", errors="ignore").strip()

    if proc.returncode != 0:
        msg = f"js_crawler_failed:exit={proc.returncode}"
        if err:
            msg += f":{err.splitlines()[-1][:300]}"
        return {"status": STATUS_FAILED, "error": msg}

    if not out:
        return {"status": STATUS_FAILED, "error": "js_crawler_failed:empty_output"}

    try:
        payload = json.loads(out)
    except Exception as e:
        return {"status": STATUS_FAILED, "error": f"js_crawler_failed:invalid_json:{e}"}

    return payload


def save_favicon_file(raw: bytes, mime: str, fav_url: str, icons_dir: Path, site: str):
    ext = ext_from_ct(mime, fav_url)
    out = icons_dir / f"{site}{ext}"
    out.write_bytes(raw)
    return str(out)


async def process_site(site: str, icons_dir: Path, timeout_sec: int, settle_ms: int, node_bin: str, js_crawler_path: Path):
    row = {
        "attempted_url": "",
        "homepage_url": "",
        "favicon_url": "",
        "favicon_file": "",
        "favicon_blob": b"",
        "content_type": "",
        "error": "",
        "status": STATUS_FAILED,
    }

    try:
        payload = await run_js_crawler(site, timeout_sec, settle_ms, node_bin, js_crawler_path)
        row["attempted_url"] = str(payload.get("attempted_url") or "")
        row["homepage_url"] = str(payload.get("homepage_url") or "")
        row["favicon_url"] = str(payload.get("favicon_url") or "")

        if int(payload.get("status", STATUS_FAILED)) != STATUS_SUCCESS:
            row["error"] = str(payload.get("error") or "failed")
            return row

        ctype = str(payload.get("content_type") or "")
        fav_b64 = payload.get("favicon_base64") or ""
        try:
            raw = base64.b64decode(fav_b64)
        except Exception:
            row["error"] = "invalid_favicon_base64"
            return row
        if not raw:
            row["error"] = "empty_body"
            return row

        row["favicon_blob"] = raw
        row["content_type"] = ctype or ""
        row["favicon_file"] = save_favicon_file(raw, ctype, row["favicon_url"] or "", icons_dir, site)
        row["status"] = STATUS_SUCCESS
        row["error"] = ""
        return row
    except Exception as e:
        row["error"] = f"unexpected:{e}"
        return row


async def worker_loop(
    name: str,
    db: CrawlDB,
    icons_dir: Path,
    timeout_sec: int,
    settle_ms: int,
    node_bin: str,
    js_crawler_path: Path,
    progress_state: dict,
    progress_lock: asyncio.Lock,
    total_to_process: int,
):
    while True:
        claimed = db.claim_next()
        if claimed is None:
            return
        site_id, site = claimed

        try:
            result = await process_site(site, icons_dir, timeout_sec, settle_ms, node_bin, js_crawler_path)
        except Exception as e:
            db.mark_failed(site_id=site_id, attempted_url="", homepage_url="", favicon_url="", error=f"worker_error:{e}"[:2000])
            async with progress_lock:
                progress_state["done"] += 1
                done = progress_state["done"]
            print(f"[error] [{done}/{total_to_process}] {site} -> {e}")
            continue

        if result["status"] == STATUS_SUCCESS:
            db.mark_success(
                site_id=site_id,
                attempted_url=result["attempted_url"],
                homepage_url=result["homepage_url"],
                favicon_url=result["favicon_url"],
                favicon_file=result["favicon_file"],
                favicon_blob=result["favicon_blob"],
                content_type=result["content_type"],
            )
            async with progress_lock:
                progress_state["done"] += 1
                done = progress_state["done"]
            print(f"[ok] [{done}/{total_to_process}] {site}")
        else:
            db.mark_failed(
                site_id=site_id,
                attempted_url=result["attempted_url"],
                homepage_url=result["homepage_url"],
                favicon_url=result["favicon_url"],
                error=result["error"] or "failed",
            )
            async with progress_lock:
                progress_state["done"] += 1
                done = progress_state["done"]
            print(f"[failed] [{done}/{total_to_process}] {site} -> {result['error']}")
            if _is_browser_closed(result.get("error", "")):
                return


async def main():
    in_path = INPUT_PATH.resolve()
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    sites = read_sites(in_path)

    out_dir = OUTPUT_DIR.resolve() / in_path.stem
    icons_dir = out_dir / "icons"
    out_dir.mkdir(parents=True, exist_ok=True)
    icons_dir.mkdir(parents=True, exist_ok=True)

    js_crawler_path = JS_CRAWLER_PATH.resolve()
    if not js_crawler_path.exists():
        raise SystemExit(f"JS crawler not found: {js_crawler_path}")

    running_dbs_dir = Path(__file__).resolve().parent / "running_dbs"
    running_dbs_dir.mkdir(parents=True, exist_ok=True)
    db_path = running_dbs_dir / f"{in_path.stem}.db"
    db = CrawlDB(db_path)

    db.seed_sites(sites)
    db.requeue_in_progress()

    db.conn.execute(
        "UPDATE sites SET status = ?, updated_at = ? WHERE status = ?",
        (STATUS_PENDING, utc_now(), STATUS_FAILED),
    )

    stats = db.stats()
    print(f"DB: {db_path}")
    print(f"pending={stats[STATUS_PENDING]} in_progress={stats[STATUS_IN_PROGRESS]} success={stats[STATUS_SUCCESS]} failed={stats[STATUS_FAILED]}")

    total_to_process = stats[STATUS_PENDING]
    progress_state = {"done": 0}
    progress_lock = asyncio.Lock()

    workers = [
        worker_loop(
            f"w{i+1}",
            db,
            icons_dir,
            TIMEOUT_SEC,
            SETTLE_MS,
            'node',
            js_crawler_path,
            progress_state,
            progress_lock,
            total_to_process,
        )
        for i in range(CONCURRENCY)
    ]
    await asyncio.gather(*workers, return_exceptions=True)

    db.export_csv(out_dir / "results.csv")
    db.export_jsonl(out_dir / "results.jsonl")

    final = db.stats()
    print("Done.")
    print(f"pending={final[STATUS_PENDING]} in_progress={final[STATUS_IN_PROGRESS]} success={final[STATUS_SUCCESS]} failed={final[STATUS_FAILED]}")
    print(f"Output: {out_dir}")
    db.close()


if __name__ == "__main__":
    asyncio.run(main())
