"""
Domain categorisation for third-party script analysis.

Strategy (applied in order):
  1. Local cache (persistent JSON) — fastest
  2. CF Radar top-100 pre-fetched map (categories available for globally top-100 domains)
  3. CF Radar ranking CSV (top-1M) — for rank/popularity signal only, no category
  4. Pattern-based heuristics — keyword matching on domain name / subdomain
  5. TLD fallback

CF Radar does not provide a per-domain content-category API beyond the top-100 list.
For the long tail, we rely on pattern matching which is sufficient for the common
third-party script domains (analytics, ads, CDN, social, payments).
"""

import os
import sys
import json
import time
import re
import traceback
import io

import requests
import tldextract

_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.scripts.config import CF_RADAR_API, CF_RADAR_BASE, TLD_CATEGORY_MAP

# ── Cache ─────────────────────────────────────────────────────────────────────

_CACHE_PATH = os.path.join(_HERE, ".domain_category_cache.json")
_RANK_PATH  = os.path.join(_HERE, ".domain_rank_cache.json")
_cache: dict[str, str] = {}
_rank:  dict[str, int] = {}   # domain → position (1-based line index as proxy for rank)
_rank_loaded = False           # flag so we attempt the download exactly once per session


def _load_cache():
    global _cache, _rank, _rank_loaded
    try:
        if os.path.exists(_CACHE_PATH):
            _cache = json.load(open(_CACHE_PATH))
    except Exception:
        _cache = {}
    try:
        if os.path.exists(_RANK_PATH):
            _rank = json.load(open(_RANK_PATH))
            _rank_loaded = True   # already on disk — no need to re-download
    except Exception:
        _rank = {}


def _save_cache():
    try:
        json.dump(_cache, open(_CACHE_PATH, "w"), indent=2)
    except Exception:
        traceback.print_exc()


_load_cache()

# ── CF Radar top-100 fetch ────────────────────────────────────────────────────

_cf_top100: dict[str, str] = {}  # domain → category name


def _fetch_cf_top100():
    """Fetch CF Radar top-100 domains with their categories (one-time per session)."""
    global _cf_top100
    if _cf_top100:
        return
    token = CF_RADAR_API
    if not token:
        return
    try:
        resp = requests.get(
            f"{CF_RADAR_BASE}/ranking/top",
            params={"limit": 100},
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if resp.status_code != 200:
            return
        entries = resp.json().get("result", {}).get("top_0", [])
        for e in entries:
            dom = e.get("domain", "").lower()
            cats = e.get("categories", [])
            if dom and cats:
                _cf_top100[dom] = cats[0].get("name", "Other/Unknown")
    except Exception:
        traceback.print_exc()


# ── CF Radar ranking CSV (rank/popularity only) ───────────────────────────────

def _ensure_rank_cache():
    """
    Download CF Radar top-1M domain list if not already cached locally.
    The CSV has a single 'domain' column (no rank numbers).
    Stores {registered_domain: position} where position is 1-based line index.
    Attempts the download exactly once per process; marks _rank_loaded=True on
    completion or failure so callers never loop.
    """
    global _rank, _rank_loaded
    if _rank_loaded:
        return
    _rank_loaded = True   # set immediately — prevents re-entry on any code path

    token = CF_RADAR_API
    if not token:
        return
    try:
        r = requests.get(
            f"{CF_RADAR_BASE}/datasets",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if r.status_code != 200:
            return
        datasets = r.json().get("result", {}).get("datasets", [])
        top1m = next((d for d in datasets if d.get("alias") == "ranking_top_1000000"), None)
        if not top1m:
            return

        r2 = requests.post(
            f"{CF_RADAR_BASE}/datasets/download",
            json={"datasetId": top1m["id"]},
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=15,
        )
        if r2.status_code != 200:
            return
        url = r2.json().get("result", {}).get("dataset", {}).get("url")
        if not url:
            return

        print("  Downloading CF Radar top-1M domain list (~8MB, one-time)...")
        r3 = requests.get(url, timeout=120)
        if r3.status_code != 200:
            return

        # Format: header line "domain", then one domain per line (no rank column)
        lines = r3.content.decode("utf-8", errors="ignore").splitlines()
        pos = 1
        for line in lines:
            domain = line.strip().lower()
            if not domain or domain == "domain":   # skip header
                continue
            ext = tldextract.extract(domain)
            reg = ext.registered_domain or domain
            if reg and reg not in _rank:           # keep first (lowest index = highest rank)
                _rank[reg] = pos
            pos += 1

        json.dump(_rank, open(_RANK_PATH, "w"))
        print(f"  Cached {len(_rank):,} domain ranks.")
    except Exception:
        traceback.print_exc()


# ── Pattern-based heuristic categorisation ───────────────────────────────────

# Each entry: (compiled regex on registered_domain or fqdn, category)
_PATTERNS: list[tuple[re.Pattern, str]] = [p for p in [
    # Analytics & monitoring
    (re.compile(r"(google-analytics|googletagmanager|gtm\.|gtag\.|analytics|mixpanel|"
                r"segment\.|amplitude\.|hotjar|clarity\.|heap\.|pendo\.|fullstory|"
                r"newrelic|datadog|sentry\.io|bugsnag|logrocket|mouseflow|"
                r"statcounter|matomo|plausible)"), "Analytics"),
    # Advertising & ad tech
    (re.compile(r"(doubleclick|googlesyndication|googleadservices|adnxs|"
                r"adsystem|adtech|amazon-adsystem|criteo|outbrain|taboola|"
                r"pubmatic|rubiconproject|openx|spotxchange|lijit|"
                r"advertising|adsense|adserver|adroll|mediamath|"
                r"moatads|quantserve|scorecardresearch|comscore)"), "Advertising"),
    # Social media
    (re.compile(r"(facebook\.net|facebook\.com|fbcdn|instagram|twitter|"
                r"linkedin|pinterest|tiktok|snapchat|reddit|tumblr|"
                r"social|shareaholic|addthis|sharethis)"), "Social Media"),
    # CDN & static assets
    (re.compile(r"(cloudfront|akamai|fastly|cloudflare|jsdelivr|unpkg|"
                r"cdnjs|bootstrapcdn|stackpath|azureedge|"
                r"gstatic|googleusercontent|ftcdn|imgix|"
                r"bunnycdn|b-cdn\.net|keycdn)"), "CDN/Static"),
    # Video & media
    (re.compile(r"(youtube|youtu\.be|vimeo|brightcove|jwplayer|"
                r"dailymotion|twitch|wistia|vidyard|videojs|"
                r"kaltura|ooyala|flowplayer)"), "Video/Media"),
    # Payments & finance
    (re.compile(r"(stripe|paypal|braintree|square|adyen|klarna|"
                r"checkout\.com|recurly|chargebee|zuora|"
                r"bitcoin|coinbase|crypto)"), "Finance/Payments"),
    # CMS & web frameworks
    (re.compile(r"(wordpress|wp\.com|wix\.com|squarespace|shopify|"
                r"webflow|ghost\.io|drupal|joomla|magento|"
                r"prestashop|opencart|bigcommerce)"), "CMS/E-commerce"),
    # Tag management & marketing
    (re.compile(r"(tealium|ensighten|signal|qubit|optimizely|"
                r"abtasty|vwo\.com|convert\.com|monetate|"
                r"bazaarvoice|yotpo|trustpilot|reviews)"), "Marketing/Tag management"),
    # Security & fraud
    (re.compile(r"(recaptcha|hcaptcha|cloudflare\.com|"
                r"fraud|bot-?detect|device-?fingerprint|"
                r"threatmetrix|kount|sift|signifyd)"), "Security"),
    # Customer support
    (re.compile(r"(zendesk|intercom|freshdesk|livechat|"
                r"olark|drift\.com|tidio|crisp\.chat|"
                r"hubspot|salesforce|crm)"), "Customer Support/CRM"),
    # Maps & location
    (re.compile(r"(maps\.google|mapbox|openstreetmap|"
                r"here\.com|tomtom|foursquare)"), "Maps/Location"),
    # Fonts & UI
    (re.compile(r"(fonts\.google|fonts\.gstatic|typekit|"
                r"fontawesome|adobe-fonts|fonts\.com)"), "Fonts/UI"),
    # A/B testing
    (re.compile(r"(optimizely|abtasty|vwo|kameleoon|"
                r"conductrics|split\.io|launchdarkly)"), "A/B Testing"),
    # Search
    (re.compile(r"(algolia|elasticsearch|swiftype|doofinder|"
                r"google.*search|bing.*search)"), "Search"),
]]

_EXACT: dict[str, str] = {
    "jquery.com": "JavaScript Library",
    "jquery.net": "JavaScript Library",
    "reactjs.org": "JavaScript Library",
    "vuejs.org": "JavaScript Library",
    "angular.io": "JavaScript Library",
    "lodash.com": "JavaScript Library",
    "momentjs.com": "JavaScript Library",
    "axios-http.com": "JavaScript Library",
    "polyfill.io": "JavaScript Library",
    "google.com": "Search Engines",
    "googleapis.com": "Technology",
    "amazon.com": "E-commerce",
    "amazonaws.com": "Technology/Cloud",
    "microsoft.com": "Technology",
    "apple.com": "Technology",
}


def _pattern_categorise(domain: str) -> str | None:
    domain_lower = domain.lower()
    if domain_lower in _EXACT:
        return _EXACT[domain_lower]
    for pattern, cat in _PATTERNS:
        if pattern.search(domain_lower):
            return cat
    return None


def _tld_fallback(domain: str) -> str:
    ext = tldextract.extract(domain)
    tld = ext.suffix.lower().split(".")[-1] if ext.suffix else ""
    return TLD_CATEGORY_MAP.get(tld, "Other/Unknown")


# ── Public API ────────────────────────────────────────────────────────────────

def categorise(domain: str) -> str:
    """
    Return category for a domain. Order: cache → CF top-100 → pattern → TLD.
    """
    ext = tldextract.extract(domain)
    key = ext.registered_domain.lower() if ext.registered_domain else domain.lower()

    if key in _cache:
        return _cache[key]

    # CF Radar top-100 (exact match on registered domain)
    _fetch_cf_top100()
    if key in _cf_top100:
        cat = _cf_top100[key]
        _cache[key] = cat
        return cat

    # Pattern matching
    cat = _pattern_categorise(key) or _tld_fallback(key)
    _cache[key] = cat
    return cat


def categorise_bulk(domains: list[str], verbose: bool = False) -> dict[str, str]:
    """Categorise a list of domains, returning {domain: category}. Saves cache on completion."""
    _fetch_cf_top100()

    result: dict[str, str] = {}
    uncached = []
    for d in domains:
        ext = tldextract.extract(d)
        key = ext.registered_domain.lower() if ext.registered_domain else d.lower()
        if key in _cache:
            result[d] = _cache[key]
        else:
            uncached.append(d)

    if verbose and uncached:
        print(f"  Categorising {len(uncached)} domains ({len(result)} cached)...")

    for d in uncached:
        result[d] = categorise(d)

    _save_cache()
    return result


def get_rank(domain: str) -> int | None:
    """Return CF Radar rank for a domain (1 = most popular), or None if not in top-1M."""
    _ensure_rank_cache()
    ext = tldextract.extract(domain)
    key = ext.registered_domain.lower() if ext.registered_domain else domain.lower()
    return _rank.get(key)


def flush_cache():
    _save_cache()
