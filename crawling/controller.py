#!/usr/bin/env python3
import argparse
import csv
import logging
import sqlite3
import subprocess
import sys
import json
import os
import signal
import shutil
import tarfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock
from urllib import request
from urllib.error import URLError
from dotenv import load_dotenv

COUNTRY_NAMES_DICT = {
    'United Arab Emirates': 'AE',
    'United States': 'US',
    'India': 'IN',
    'Germany': 'DE',
    'Algeria': 'DZ',
}

RESULT_PREFIX = '[crawl_url_result]'


def _is_debug_enabled() -> bool:
    value = os.environ.get('DEBUG', 'false').strip().lower()
    return value in {'1', 'true', 'yes', 'on'}


def debug_log(message: str, level: str = 'info') -> None:
    if not _is_debug_enabled():
        return

    log_fn = getattr(logging, level, logging.info)
    log_fn(message)


def terminate_process_group(process, grace_seconds: int = 10) -> None:
    """Best-effort termination of a spawned process and its process group."""
    if process is None or process.poll() is not None:
        return

    try:
        pgid = os.getpgid(process.pid)
    except Exception:
        pgid = None

    if pgid is not None:
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except Exception:
            pass
    else:
        try:
            process.terminate()
        except Exception:
            pass

    try:
        process.wait(timeout=grace_seconds)
        return
    except subprocess.TimeoutExpired:
        logging.warning(f'Force-killing stuck crawl process pid={process.pid}')
    except Exception:
        return

    if pgid is not None:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except Exception:
            pass
    else:
        try:
            process.kill()
        except Exception:
            return

    try:
        process.wait(timeout=5)
    except Exception:
        pass

def parse_crawl_result(stdout: str):
    """Parse crawl result payload from crawl_url.js stdout."""
    for line in reversed(stdout.splitlines()):
        if line.startswith(RESULT_PREFIX):
            payload = line[len(RESULT_PREFIX):]
            data = json.loads(payload)
            success = bool(data.get('success', False))
            dom_ts = data.get('domContentLoadedTs')
            if dom_ts is not None:
                dom_ts = str(dom_ts)
            return success, dom_ts
    return None

def cleanup_proxy_port(proxy_port: int) -> None:
    """Kill any stale process listening on the given proxy port."""
    fuser_bin = shutil.which('fuser')
    if fuser_bin:
        term_result = subprocess.run(
            [fuser_bin, '-k', '-TERM', f'{proxy_port}/tcp'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if term_result.returncode == 0:
            logging.warning(f'Killed stale listener on proxy port {proxy_port} before crawl start')
            time.sleep(0.5)
            subprocess.run(
                [fuser_bin, '-k', f'{proxy_port}/tcp'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
        return

    lsof_bin = shutil.which('lsof')
    if lsof_bin:
        pid_result = subprocess.run(
            [lsof_bin, '-ti', f'tcp:{proxy_port}'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        pids = [int(pid.strip()) for pid in pid_result.stdout.splitlines() if pid.strip().isdigit()]
        if not pids:
            return

        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
        logging.warning(f'Killed stale listener on proxy port {proxy_port} before crawl start')
        time.sleep(0.5)
        for pid in pids:
            try:
                os.kill(pid, 0)
            except OSError:
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

def restore_browser_profile_if_needed(browser_profile_path: Path, browser_profile_archive_path: Path) -> None:
    if browser_profile_path.exists() or not browser_profile_archive_path.exists():
        return

    with tarfile.open(browser_profile_archive_path, mode='r:gz') as tar:
        # Archive is produced locally by this crawler; use fully_trusted to keep
        # historical behavior and avoid platform-specific extraction warnings.
        try:
            tar.extractall(path=browser_profile_path.parent, filter='fully_trusted')
        except TypeError:
            tar.extractall(path=browser_profile_path.parent)

    if not browser_profile_path.exists():
        raise RuntimeError(f'Failed to restore browser profile from {browser_profile_archive_path}')

    browser_profile_archive_path.unlink()


def archive_browser_profile(browser_profile_path: Path, browser_profile_archive_path: Path) -> None:
    if not browser_profile_path.exists() or not browser_profile_path.is_dir():
        return

    tmp_archive_path = Path(f'{browser_profile_archive_path}.tmp')
    if tmp_archive_path.exists():
        tmp_archive_path.unlink()

    with tarfile.open(tmp_archive_path, mode='w:gz') as tar:
        tar.add(browser_profile_path, arcname=browser_profile_path.name)

    if browser_profile_archive_path.exists():
        browser_profile_archive_path.unlink()

    tmp_archive_path.replace(browser_profile_archive_path)
    shutil.rmtree(browser_profile_path)

def run_crawl(node_script, output_path, browser_profile_path, url, country, max_attempts, proxy_port):
    cmd = [
        'node',
        str(node_script),
        '--output_path',
        str(output_path),
        '--browser_profile_path',
        str(browser_profile_path),
        '--url',
        url,
        '--country',
        country,
        '--maxAttempts',
        str(max_attempts),
        '--proxy_port',
        str(proxy_port),
    ]

    timeout_seconds = int(os.environ.get('CRAWL_TIMEOUT_SECONDS'))
    process = None

    try:
        cleanup_proxy_port(proxy_port)

        # Run in a new process group so timeout handling can terminate child processes too.
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=os.setsid,
        )

        stdout, stderr = process.communicate(timeout=timeout_seconds)

        if process.returncode != 0:
            err_text = (stderr or stdout or '').strip()
            debug_log(f'Crawl process failed for {url} (rc={process.returncode}): {err_text}', level='error')
            parsed = parse_crawl_result(stdout or '')
            if parsed is not None:
                return parsed
            return False, None

        parsed = parse_crawl_result(stdout or '')
        if parsed is not None:
            return parsed
        return True, None

    except subprocess.TimeoutExpired:
        debug_log(f'Crawl process timed out after {timeout_seconds}s for {url}', level='error')
        if process is not None:
            terminate_process_group(process)
        return False, None

    except Exception as exc:
        debug_log(f'Unexpected error in run_crawl for {url}: {exc}', level='error')
        if process is not None and process.poll() is None:
            terminate_process_group(process)
        return False, None

def get_country_from_ip() -> str:
    try:
        with request.urlopen('https://api64.ipify.org?format=json', timeout=10) as resp:
            ip_data = json.loads(resp.read().decode('utf-8'))
        ip = ip_data.get('ip')
        if not ip:
            raise ValueError('No IP returned by ipify')

        with request.urlopen(f'http://ip-api.com/json/{ip}', timeout=10) as resp:
            location_data = json.loads(resp.read().decode('utf-8'))

        country = location_data.get('country')
        if not country:
            raise ValueError('No country returned by ip-api')

        return COUNTRY_NAMES_DICT.get(country, str(country).upper())
    except (URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f'Error fetching country from IP: {exc}') from exc

def get_running_db_path(csv_path: Path) -> Path:
    return csv_path.parent / f'running_{csv_path.stem}.db'

def initialize_db_if_needed(db_path: Path, csv_path: Path) -> None:
    """Create runtime DB from CSV if DB does not exist."""
    if db_path.exists():
        logging.info(f'Continuing from existing DB: {db_path.name}')
        return

    if not csv_path.exists():
        raise FileNotFoundError(f'CSV file not found: {csv_path}')

    logging.info(f'Creating runtime DB: {db_path.name}')
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            '''
            CREATE TABLE crawl_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bucket TEXT NOT NULL,
                site TEXT NOT NULL,
                url TEXT NOT NULL,
                attempt_id TEXT NOT NULL,
                run_status INTEGER NOT NULL DEFAULT 0,
                domcontentloaded_ts TEXT,
                number_of_trials INTEGER NOT NULL DEFAULT 0
            )
            '''
        )
        conn.execute(
            'CREATE UNIQUE INDEX idx_crawl_tasks_unique ON crawl_tasks(bucket, site, url, attempt_id)'
        )

        with csv_path.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows_to_insert = [
                (row['Bucket'], row['site'], row['url'], row['Attempt_ID'], 0)
                for row in reader
            ]

        conn.executemany(
            'INSERT INTO crawl_tasks(bucket, site, url, attempt_id, run_status) VALUES (?, ?, ?, ?, ?)',
            rows_to_insert,
        )
        conn.commit()
    finally:
        conn.close()

def ensure_db_schema(db_path: Path) -> None:
    """Ensure expected columns exist for already-created runtime DBs."""
    conn = sqlite3.connect(db_path)
    try:
        columns = conn.execute('PRAGMA table_info(crawl_tasks)').fetchall()
        col_names = {row[1] for row in columns}
        if 'domcontentloaded_ts' not in col_names:
            conn.execute('ALTER TABLE crawl_tasks ADD COLUMN domcontentloaded_ts TEXT')
            conn.commit()
            logging.info('Added column domcontentloaded_ts to crawl_tasks')
        if 'number_of_trials' not in col_names:
            conn.execute('ALTER TABLE crawl_tasks ADD COLUMN number_of_trials INTEGER NOT NULL DEFAULT 0')
            conn.execute(
                '''
                UPDATE crawl_tasks
                SET number_of_trials = CASE
                    WHEN run_status IN (0, 1) THEN 0
                    WHEN run_status IN (-1, 2) THEN 1
                    ELSE 0
                END
                '''
            )
            conn.commit()
            logging.info(
                'Added column number_of_trials to crawl_tasks and backfilled using run_status '
                '(0/1->0, -1/2->1)'
            )
    finally:
        conn.close()

def get_status_counts(conn: sqlite3.Connection) -> dict[int, int]:
    rows = conn.execute(
        'SELECT run_status, COUNT(*) FROM crawl_tasks GROUP BY run_status'
    ).fetchall()
    counts = {0: 0, 1: 0, 2: 0, -1: 0}
    for status, count in rows:
        counts[int(status)] = int(count)
    return counts

def claim_next_site(conn: sqlite3.Connection, max_trials_in_crawl: int):
    """Atomically claim eligible pending rows for one site by moving run_status 0 -> 1."""
    try:
        conn.execute('BEGIN IMMEDIATE')
        site_row = conn.execute(
            '''
            SELECT bucket, site
            FROM crawl_tasks
            WHERE run_status = 0
              AND number_of_trials < ?
            ORDER BY id
            LIMIT 1
            '''
        ,
            (max_trials_in_crawl,),
        ).fetchone()

        if site_row is None:
            conn.execute('COMMIT')
            return None

        bucket, site = site_row
        cur = conn.execute(
            '''
            UPDATE crawl_tasks
            SET run_status = 1
                        WHERE bucket = ?
                            AND site = ?
                            AND run_status = 0
                            AND number_of_trials < ?
            ''',
                        (bucket, site, max_trials_in_crawl),
        )
        conn.execute('COMMIT')

        if cur.rowcount >= 1:
            return (bucket, site)
        return None
    except Exception:
        conn.execute('ROLLBACK')
        raise

def get_claimed_site_tasks(conn: sqlite3.Connection, bucket: str, site: str):
    return conn.execute(
        '''
        SELECT id, url, attempt_id
        FROM crawl_tasks
        WHERE bucket = ? AND site = ? AND run_status = 1
        ORDER BY id
        ''',
        (bucket, site),
    ).fetchall()

def mark_site_in_progress_failed(conn: sqlite3.Connection, bucket: str, site: str) -> None:
    conn.execute(
        '''
        UPDATE crawl_tasks
        SET run_status = -1
        WHERE bucket = ? AND site = ? AND run_status = 1
        ''',
        (bucket, site),
    )
    conn.commit()

def set_task_status(conn: sqlite3.Connection, task_id: int, status: int, domcontentloaded_ts=None) -> None:
    conn.execute(
        '''
        UPDATE crawl_tasks
        SET run_status = ?,
            domcontentloaded_ts = ?,
            number_of_trials = number_of_trials + 1
        WHERE id = ?
        ''',
        (status, domcontentloaded_ts, task_id),
    )
    conn.commit()

def reset_in_progress_tasks(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute('UPDATE crawl_tasks SET run_status = 0 WHERE run_status = 1')
        conn.commit()
        if cur.rowcount > 0:
            logging.warning(f'Reset {cur.rowcount} stale in-progress tasks from status=1 to status=0')
    finally:
        conn.close()


def main() -> int:
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(threadName)-12s] %(levelname)s: %(message)s'
    )
    
    # Load environment variables from .env
    load_dotenv()
    
    # Validate required environment variables
    required_env = [
        'MAX_ATTEMPTS',
        'PROXY_PORT',
        'MAX_CORES',
        'CRAWL_TIMEOUT_SECONDS',
        'MAX_TRIALS_IN_CRAWL',
        'MAX_SITES_TO_CRAWL',
    ]
    missing = [var for var in required_env if var not in os.environ]
    if missing:
        logging.error(f'Missing required env variables: {", ".join(missing)}')
        return 1
    
    parser = argparse.ArgumentParser(description='Controller that reads D1/D2/D3 CSV and crawls each URL.')
    parser.add_argument('csv_file', help='Path to CSV file with Bucket,site,url,Attempt_ID columns')
    args = parser.parse_args()

    try:
        country = get_country_from_ip()
        logging.info(f'Detected country from IP: {country}')
    except RuntimeError as exc:
        logging.error(str(exc))
        return 1

    csv_path = Path(args.csv_file).resolve()
    running_db = get_running_db_path(csv_path)

    try:
        initialize_db_if_needed(running_db, csv_path)
        ensure_db_schema(running_db)
    except (FileNotFoundError, KeyError, sqlite3.DatabaseError) as exc:
        logging.error(str(exc))
        return 1

    # Recover from interrupted runs so tasks are not stuck forever in status=1.
    reset_in_progress_tasks(running_db)

    # Use running file for all operations
    script_path = (Path(__file__).resolve().parent / 'crawl_url.js').resolve()
    snapshots_root = Path(__file__).resolve().parents[1] / 'data' / 'snapshots'
    browser_profile_template = (Path(__file__).resolve().parent / 'resources' / 'shields-down-profile').resolve()

    if not script_path.exists():
        logging.error(f'crawl_url.js not found: {script_path}')
        return 1

    if not browser_profile_template.exists():
        logging.error(f'Browser profile template not found: {browser_profile_template}')
        return 1

    conn = sqlite3.connect(running_db)
    try:
        total = conn.execute('SELECT COUNT(*) FROM crawl_tasks').fetchone()[0]
        counts = get_status_counts(conn)
    finally:
        conn.close()

    if total == 0:
        logging.info('No crawlable rows found in DB.')
        return 0

    already_done = counts.get(2, 0)
    under_processing = counts.get(1, 0)
    failed = counts.get(-1, 0)
    to_process = counts.get(0, 0)

    logging.info(f'Loaded {total} rows from {running_db}')
    logging.info(f'  Success (2): {already_done}')
    logging.info(f'  Pending (0): {to_process}')
    logging.info(f'  Under processing (1): {under_processing}')
    logging.info(f'  Failed (-1): {failed}')

    if to_process == 0:
        logging.info('No pending rows (status=0) to process.')
        return 0

    max_attempts = int(os.environ['MAX_ATTEMPTS'])
    max_trials_in_crawl = int(os.environ['MAX_TRIALS_IN_CRAWL'])
    max_sites_to_crawl = int(os.environ['MAX_SITES_TO_CRAWL'])
    base_proxy_port = int(os.environ['PROXY_PORT'])
    max_cores = int(os.environ['MAX_CORES'])

    if max_trials_in_crawl <= 0:
        logging.error('MAX_TRIALS_IN_CRAWL must be a positive integer')
        return 1
    if max_sites_to_crawl < 0:
        logging.error('MAX_SITES_TO_CRAWL must be >= 0 (0 means no cap)')
        return 1

    conn = sqlite3.connect(running_db)
    try:
        pending_sites = conn.execute(
            '''
            SELECT COUNT(DISTINCT bucket || '|' || site)
            FROM crawl_tasks
            WHERE run_status = 0
              AND number_of_trials < ?
            '''
            ,
            (max_trials_in_crawl,),
        ).fetchone()[0]
    finally:
        conn.close()

    if pending_sites == 0:
        logging.info('No eligible sites to process (pending rows under trial limit).')
        return 0

    if max_sites_to_crawl == 0:
        total_sites_to_claim = pending_sites
    else:
        total_sites_to_claim = min(pending_sites, max_sites_to_crawl)

    num_workers = min(max_cores, total_sites_to_claim)
    claimed_sites_count = 0
    claimed_sites_lock = Lock()

    logging.info(f'Eligible pending sites: {pending_sites}')
    logging.info(f'Site cap for this run (MAX_SITES_TO_CRAWL): {total_sites_to_claim}')
    logging.info(f'Using {num_workers} parallel crawlers (site-level)')

    def worker(core_id: int) -> None:
        nonlocal claimed_sites_count
        proxy_port = base_proxy_port + core_id
        conn = sqlite3.connect(running_db, timeout=30)

        try:
            while True:
                with claimed_sites_lock:
                    if claimed_sites_count >= total_sites_to_claim:
                        return
                    claimed_site = claim_next_site(conn, max_trials_in_crawl)
                    if claimed_site is None:
                        return
                    claimed_sites_count += 1
                    claimed_so_far = claimed_sites_count

                bucket, site = claimed_site
                try:
                    claimed_rows = get_claimed_site_tasks(conn, bucket, site)
                    if not claimed_rows:
                        continue

                    site_dir = snapshots_root / country / bucket / site

                    logging.info(
                        f'[core {core_id}] Claimed site {bucket}/{site} '
                        f'({claimed_so_far}/{total_sites_to_claim}) with {len(claimed_rows)} task(s)'
                    )

                    attempt_groups = defaultdict(list)
                    for task_id, url, attempt_id in claimed_rows:
                        attempt_groups[str(attempt_id)].append((task_id, url))

                    ordered_attempt_ids = [str(attempt_id) for _, _, attempt_id in claimed_rows]
                    ordered_attempt_ids = list(dict.fromkeys(ordered_attempt_ids))

                    for attempt_id in ordered_attempt_ids:
                        task_entries = attempt_groups[attempt_id]
                        attempt_dir = site_dir / attempt_id
                        attempt_dir.mkdir(parents=True, exist_ok=True)
                        browser_profile_path = attempt_dir / 'browser_profile'
                        browser_profile_archive_path = attempt_dir / 'browser_profile.tar.gz'

                        if not browser_profile_path.exists():
                            if browser_profile_archive_path.exists():
                                try:
                                    restore_browser_profile_if_needed(browser_profile_path, browser_profile_archive_path)
                                except Exception as exc:
                                    logging.warning(
                                        f'[core {core_id}] Failed to restore archived browser profile at {browser_profile_archive_path}: {exc}. '
                                        'Rebuilding from template.'
                                    )
                                    try:
                                        browser_profile_archive_path.unlink()
                                    except Exception:
                                        pass

                        if not browser_profile_path.exists():
                            try:
                                shutil.copytree(browser_profile_template, browser_profile_path)
                            except FileExistsError:
                                pass

                        try:
                            for task_id, url in task_entries:
                                debug_log(f'[core {core_id}] Crawling {bucket}/{site}/{attempt_id} -> {url} (proxy {proxy_port})')
                                ok, domcontentloaded_ts = run_crawl(
                                    script_path,
                                    attempt_dir,
                                    browser_profile_path,
                                    url,
                                    country,
                                    max_attempts,
                                    proxy_port,
                                )

                                set_task_status(conn, task_id, 2 if ok else -1, domcontentloaded_ts if ok else None)

                                if ok:
                                    debug_log(f'[core {core_id}] Marked status=2 (success) in DB with domcontentloaded_ts={domcontentloaded_ts}')
                                else:
                                    debug_log(f'[core {core_id}] Marked status=-1 (failed) in DB', level='error')
                        finally:
                            try:
                                archive_browser_profile(browser_profile_path, browser_profile_archive_path)
                            except Exception as exc:
                                logging.warning(
                                    f'[core {core_id}] Failed to archive browser profile at {browser_profile_path}: {exc}'
                                )

                except Exception as e:
                    logging.exception(f'[core {core_id}] Unexpected error while processing site {bucket}/{site}: {e}')
                    try:
                        mark_site_in_progress_failed(conn, bucket, site)
                    except Exception:
                        logging.exception(f'[core {core_id}] Failed to mark remaining site tasks status=-1 after exception')

        except Exception as e:
            logging.exception(f'[core {core_id}] Fatal error in worker: {e}')
        finally:
            conn.close()

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker, core_id) for core_id in range(num_workers)]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    logging.exception(f'Worker failed: {exc}')
    except KeyboardInterrupt:
        logging.warning('Interrupted by user')
        return 1

    conn = sqlite3.connect(running_db)
    try:
        counts = get_status_counts(conn)
    finally:
        conn.close()

    print('--- Crawl Summary ---')
    print(f'Total rows: {total}')
    print(f'Success (2): {counts.get(2, 0)}')
    print(f'Pending (0): {counts.get(0, 0)}')
    print(f'Under processing (1): {counts.get(1, 0)}')
    print(f'Failed (-1): {counts.get(-1, 0)}')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
