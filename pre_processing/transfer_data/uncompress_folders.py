#!/usr/bin/env python3
import argparse, logging, shutil, subprocess, tarfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

ROOT_PATH = Path(__file__).resolve().parent
WORKERS = 8


def uncompress_archive_worker(archive_path):
    parent_dir = archive_path.parent
    tar_bin = shutil.which('tar')

    try:
        if tar_bin:
            subprocess.run(
                [
                    tar_bin,
                    '-xzf',
                    str(archive_path),
                    '-C',
                    str(parent_dir),
                ],
                check=True,
            )
        else:
            with tarfile.open(archive_path, mode='r:gz') as tar:
                tar.extractall(path=parent_dir)

        try:
            archive_path.unlink()
        except FileNotFoundError:
            pass

        return True
    except Exception:
        return False


def process_parallel(root_path):
    archives = sorted(root_path.rglob('*.tar.gz'))

    if not archives:
        return 0, 0

    uncompressed_count = 0
    failed_count = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(uncompress_archive_worker, a): a for a in archives}

        for future in as_completed(futures):
            if future.result():
                uncompressed_count += 1
            else:
                failed_count += 1

    return uncompressed_count, failed_count


def main():
    uncompressed_count, failed_count = process_parallel(ROOT_PATH)
    return 0 if failed_count == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main()) 