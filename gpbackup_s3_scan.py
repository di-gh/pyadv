#!/usr/bin/env python3
"""Обход S3-каталога с бэкапами gpbackup.

Раскладка gpbackup_s3_plugin:
    <bucket>/<prefix>/backups/<YYYYMMDD>/<YYYYMMDDHHMMSS>/
        gpbackup_<ts>_toc.yaml | _report | _metadata.sql | _config.yaml

Шаги:
  1. рекурсивно (Delimiter="/") ищем каталоги с именем из 14 цифр -> список бэкапов;
  2. для каждого бэкапа проверяем 4 известных ключа через head_object
     (без листинга файлов данных сегментов, поэтому быстро).

Примеры:
  python gpbackup_s3_scan.py -b my-bucket -p gpbackup/mycluster
  python gpbackup_s3_scan.py -b my-bucket -p gpbackup/mycluster \
      --endpoint-url https://s3.dev.local --insecure --json
"""
import argparse
import json
import logging
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger("gpbackup_scan")

TS_RE = re.compile(r"^\d{14}$")  # YYYYMMDDHHMMSS

# вид файла -> суффикс имени gpbackup_<ts>_<suffix>
FILE_SUFFIXES = {
    "toc": "toc.yaml",
    "report": "report",
    "metadata": "metadata.sql",
    "config": "config.yaml",
}
NOT_FOUND = ("404", "NoSuchKey", "NotFound")


@dataclass
class Backup:
    timestamp: str
    prefix: str  # полный prefix каталога бэкапа (с завершающим "/")
    files: dict = field(default_factory=dict)  # kind -> key


def list_dirs(s3, bucket, prefix):
    """Подкаталоги первого уровня под prefix, с защитой от зацикленной пагинации."""
    paginator = s3.get_paginator("list_objects_v2")
    seen_tokens = set()
    pages = 0
    t0 = time.time()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        pages += 1
        token = page.get("NextContinuationToken")
        if token:
            if token in seen_tokens:
                raise RuntimeError(
                    f"Зацикленная пагинация при листинге {prefix!r}: "
                    "хранилище вернуло повторяющийся NextContinuationToken"
                )
            seen_tokens.add(token)
        for cp in page.get("CommonPrefixes", []):
            yield cp["Prefix"]
    log.debug("list %s: %d стр., %.2fс", prefix, pages, time.time() - t0)


def find_backups(s3, bucket, root_prefix, max_depth=4):
    """Рекурсивно ищет каталоги-таймстампы. Возвращает Backup, отсортированные по времени."""
    if root_prefix and not root_prefix.endswith("/"):
        root_prefix += "/"
    found = []

    def walk(prefix, depth):
        if depth > max_depth:
            log.warning("достигнут max_depth=%d на %s", max_depth, prefix)
            return
        for d in list_dirs(s3, bucket, prefix):
            name = d.rstrip("/").rsplit("/", 1)[-1]
            if TS_RE.match(name):
                found.append(Backup(timestamp=name, prefix=d))
            else:
                log.debug("спуск в %s", d)
                walk(d, depth + 1)

    walk(root_prefix, 0)
    return sorted(found, key=lambda b: b.timestamp)


def fill_files(s3, bucket, backup):
    """Проверяет наличие toc/report/metadata/config через head_object."""
    for kind, suffix in FILE_SUFFIXES.items():
        key = f"{backup.prefix}gpbackup_{backup.timestamp}_{suffix}"
        try:
            s3.head_object(Bucket=bucket, Key=key)
            backup.files[kind] = key
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") not in NOT_FOUND:
                raise
    return backup


def make_client(args):
    if args.insecure:
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
        verify = False
    else:
        verify = args.ca_bundle or True  # True -> системные CA / AWS_CA_BUNDLE

    cfg = Config(
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=args.timeout,
        read_timeout=args.timeout,
        max_pool_connections=max(10, args.workers * 2),
    )
    return boto3.client(
        "s3",
        endpoint_url=args.endpoint_url,
        verify=verify,
        config=cfg,
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Список бэкапов gpbackup в S3 и их служебных файлов")
    p.add_argument("-b", "--bucket", required=True)
    p.add_argument("-p", "--prefix", default="", help="каталог внутри bucket (folder из s3 plugin config)")
    p.add_argument("--endpoint-url", default=None, help="для MinIO/Ceph/dev-сервера")
    p.add_argument("--insecure", action="store_true", help="verify=False и без InsecureRequestWarning")
    p.add_argument("--ca-bundle", default=None, help="путь к CA-сертификату (лучше, чем --insecure)")
    p.add_argument("--max-depth", type=int, default=4)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--timeout", type=int, default=30, help="таймаут соединения/чтения, сек")
    p.add_argument("--json", action="store_true", help="вывод в JSON")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    s3 = make_client(args)

    t0 = time.time()
    backups = find_backups(s3, args.bucket, args.prefix, args.max_depth)
    log.info("найдено бэкапов: %d (%.1fс)", len(backups), time.time() - t0)
    if not backups:
        log.warning("бэкапы не найдены: проверьте --prefix (%r)", args.prefix)

    t1 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        list(ex.map(lambda b: fill_files(s3, args.bucket, b), backups))
    log.info("файлы проверены за %.1fс", time.time() - t1)

    if args.json:
        out = [
            {"timestamp": b.timestamp, "prefix": b.prefix, "files": b.files}
            for b in backups
        ]
        json.dump(out, sys.stdout, ensure_ascii=False, indent=2)
        print()
    else:
        for b in backups:
            print(b.timestamp)
            for kind in FILE_SUFFIXES:
                print(f"  {kind:9} {b.files.get(kind, '— нет —')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
