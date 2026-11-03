from typing import Dict, List, Tuple, Optional, Generator
from urllib.parse import urlparse, urlunparse
from collections import defaultdict
from multiprocessing import Pool
from dotenv import load_dotenv
from tqdm import tqdm

import mysql.connector
import pandas as pd
import numpy as np
import tldextract
import traceback
import warnings
import pymysql
import json
import sys
import os

if os.path.exists("./.env"):
    load_dotenv("./.env")

warnings.filterwarnings("ignore")

DB_CONFIG = {
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "database": "crawl_analysis_last",
    "charset": os.getenv("charset", "utf8mb4"),
}

DB_CONFIG_NEW = {
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "database": "crawl_analysis_new",
    "charset": os.getenv("charset", "utf8mb4"),
}

WORKERS = int(os.getenv("WORKERS", 1))
QUICK_PEEK = False
INCLUDE_VPN = False  # Set True once crawl_analysis_new (global-VPN) is ready

# ── Visit consistency settings ────────────────────────────────────────────────
# Each etld × location is visited up to 3 times (access_id 1, 2, 3).
# MIN_VISITS: minimum visits required for an etld × location to be included.
# Pairs with fewer visits are excluded from Tier A; flagged in data quality output.
MIN_VISITS = 2

# Two-tier consistency model (see src/utils/consistency.py for full documentation):
#   TIER_A ("stable"):     signal present/identical in ALL visits
#   TIER_B ("meaningful"): signal present in AT LEAST ONE visit

TOP_N = os.getenv("TOP_N")

# Datasets in crawl_analysis_last; extend as data arrives
DATASETS = [
    ("country_specific", DB_CONFIG),
    ("global",           DB_CONFIG),
    ("country_coded",    DB_CONFIG),
]

# VPN dataset lives in crawl_analysis_new — appended at runtime when INCLUDE_VPN=True
VPN_DATASET = ("global-VPN", DB_CONFIG_NEW)

# Base output directory — each script writes to RESULTS_BASE/headers/<category>/
RESULTS_BASE = "./results/headers"

SECURITY_HEADERS = json.loads(open("./src/headers/SECURITY_HEADERS.json").read())
PERMISSIONS_POLICY = json.loads(open("./src/headers/PERMISSIONS_POLICY.json").read())

# Headers covered by the Security Lottery classification (Roth et al., USENIX Sec 2022)
SECURITY_LOTTERY = {
    "content-security-policy",
    "content-security-policy-report-only",
    "x-frame-options",
    "strict-transport-security",
    "set-cookie",
}
