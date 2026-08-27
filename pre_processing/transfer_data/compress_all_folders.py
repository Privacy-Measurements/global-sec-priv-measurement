#!/usr/bin/env python3
import shutil, subprocess, tarfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

ROOT_PATH = Path(__file__).resolve().parent
WORKERS = 8


def compress_directory(dir_path):
    archive_path = dir_path.with_suffix('.tar.gz')
    parent_dir = dir_path.parent
    tar_bin = shutil.which('tar')

    try:
        if archive_path.exists():
            return False

        if tar_bin:
            subprocess.run(
                [
                    tar_bin,
                    '-czf',
                    str(archive_path),
                    '-C',
                    str(parent_dir),
                    dir_path.name,
                ],
                check=True,
            )
        else:
            with tarfile.open(archive_path, mode='w:gz') as tar:
                tar.add(dir_path, arcname=dir_path.name)

        shutil.rmtree(dir_path)
        return True
    except Exception:
        return False


def process_parallel(root_path):
    dirs = sorted(p for p in root_path.iterdir() if p.is_dir())

    if not dirs:
        return 0, 0

    compressed_count = 0
    failed_count = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(compress_directory, d): d for d in dirs}

        for future in as_completed(futures):
            if future.result():
                compressed_count += 1
            else:
                failed_count += 1

    return compressed_count, failed_count


def main():
    compressed_count, failed_count = process_parallel(ROOT_PATH)
    return 0 if failed_count == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())