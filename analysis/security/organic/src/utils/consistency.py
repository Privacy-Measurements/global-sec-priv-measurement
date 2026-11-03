"""
Visit Consistency Utilities
============================
Implements the two-tier consistency model across all analyses:

  Tier A — "Stable":     signal present/identical in ALL visits for that etld × location.
  Tier B — "Meaningful": signal present in AT LEAST ONE visit.
  Absent:                signal never observed.

Key design decisions:
  - Unit of grouping: (etld, location) — each has up to 3 access_ids (1, 2, 3).
  - access_id comes from crawl_sessions; all downstream tables join via session_id.
  - Normalisation is applied before Tier A comparison to strip per-visit random tokens
    (CSP nonces/hashes, cookie values, URL cache-busters) so structural stability
    is not masked by intentional per-visit variation.
  - JS call categories split: deterministic APIs (fingerprinting, eval, defensive,
    cookie_storage, workers) → Tier A requires all visits; conditional APIs
    (network_exfiltration, dom_surveillance, ai_emerging, payment, geolocation_sensors)
    → Tier B is the primary tier.

Usage pattern (same across all analysis scripts):
    from src.utils.consistency import (
        load_visit_map, ConsistencyTagger, TIER_A, TIER_B, TIER_ABSENT,
        JS_TIER_A_CATEGORIES,
    )

    visit_map = load_visit_map(db_config, category)
    tagger = ConsistencyTagger(visit_map)

    # For a signal observed across sessions:
    tier = tagger.tag_binary(etld, location, observed_session_ids)

    # For a valued signal (header value, script src, etc.):
    tier = tagger.tag_value(etld, location, session_value_dict,
                            normalise_fn=normalise_csp)
"""

import re
import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pymysql
import pandas as pd
from collections import defaultdict
from typing import Callable

# ── Tier constants ────────────────────────────────────────────────────────────

TIER_A      = "stable"       # present/consistent in all visits
TIER_B      = "meaningful"   # present in at least one visit
TIER_ABSENT = "absent"       # never observed

# Minimum number of visits an etld × location must have to be included.
# Pairs with fewer visits are flagged as "insufficient_visits" and excluded
# from Tier A computation (they can still contribute to Tier B).
MIN_VISITS = 2

# ── JS call category tier assignment ─────────────────────────────────────────

# Categories where Tier A (all-visits consistency) is meaningful and reported.
# Deterministic/structural APIs that should fire on every page load if present.
JS_TIER_A_CATEGORIES = {
    "fingerprinting",
    "eval_obfuscation",
    "defensive",
    "defensive_unsafe",
    "cookie_storage",
    "workers",
}

# Categories where Tier B (at least once) is the primary tier.
# Conditional/stateful APIs that may not fire on every visit.
JS_TIER_B_PRIMARY_CATEGORIES = {
    "network_exfiltration",
    "dom_surveillance",
    "ai_emerging",
    "payment",
    "geolocation_sensors",
}

# ── Normalisation functions ───────────────────────────────────────────────────

_RE_NONCE = re.compile(r"'nonce-[A-Za-z0-9+/\-_]+=*'", re.IGNORECASE)
_RE_HASH  = re.compile(r"'(sha256|sha384|sha512)-[A-Za-z0-9+/\-_]+=*'", re.IGNORECASE)
_RE_CACHEBUST = re.compile(r"[?&](?:v|ver|version|_|cb|t|ts|rev|build|hash)=[^&\s]+",
                            re.IGNORECASE)
_RE_COOKIE_VALUE = re.compile(r"^([^=]+=)[^;]+(.*)$")


def normalise_csp(value: str) -> str:
    """
    Strip per-visit random tokens from a CSP string so structural stability
    can be assessed. Nonce values and hash values are replaced with placeholders.
    """
    if not value:
        return value
    v = _RE_NONCE.sub("'nonce-SOURCE'", value)
    v = _RE_HASH.sub("'hash-SOURCE'", v)
    return v


def normalise_cookie_header(value: str) -> str:
    """
    Strip the cookie value (after '=', before ';') from a Set-Cookie string,
    keeping the cookie name and all attributes. Ensures session token rotation
    doesn't prevent Tier A classification of consistent cookie attribute sets.
    """
    if not value:
        return value
    m = _RE_COOKIE_VALUE.match(value)
    if m:
        return m.group(1) + "__VALUE__" + m.group(2)
    return value


def normalise_script_src(value: str) -> str:
    """Strip cache-buster query parameters from script src URLs."""
    if not value:
        return value
    return _RE_CACHEBUST.sub("", value).rstrip("?&")


def normalise_noop(value: str) -> str:
    """No normalisation — value must be byte-identical for Tier A."""
    return value


# Header-specific normalisation map
HEADER_NORMALISE: dict[str, Callable[[str], str]] = {
    "content-security-policy":             normalise_csp,
    "content-security-policy-report-only": normalise_csp,
    "set-cookie":                          normalise_cookie_header,
    # All others: no normalisation needed
}


def get_header_normaliser(header_name: str) -> Callable[[str], str]:
    return HEADER_NORMALISE.get(header_name.lower(), normalise_noop)


# ── Visit map loader ──────────────────────────────────────────────────────────

VISIT_MAP_QUERY = """
    SELECT
        etld,
        location,
        id          AS session_id,
        access_id
    FROM crawl_sessions
    WHERE category = %s
      AND category NOT LIKE '%%validation'
"""


def load_visit_map(db_config: dict, category: str) -> pd.DataFrame:
    """
    Load a DataFrame of (etld, location, session_id, access_id) for the category.
    Used by ConsistencyTagger to know which sessions belong to which visit slot.

    Returns DataFrame with columns: etld, location, session_id, access_id,
    visit_count (total visits for that etld × location pair).
    """
    conn = pymysql.connect(**db_config)
    df = pd.read_sql(VISIT_MAP_QUERY, conn, params=(category,))
    conn.close()

    # Add visit_count per etld × location
    visit_counts = (df.groupby(["etld", "location"])["access_id"]
                      .nunique()
                      .reset_index(name="visit_count"))
    df = df.merge(visit_counts, on=["etld", "location"])
    return df


# ── ConsistencyTagger ─────────────────────────────────────────────────────────

class ConsistencyTagger:
    """
    Tags signals as Tier A (stable), Tier B (meaningful), or absent,
    based on the visit map for an etld × location pair.

    Initialise once per dataset, reuse across all analyses.
    """

    def __init__(self, visit_map: pd.DataFrame):
        # Build lookup: (etld, location) → {access_id: session_id}
        self._unit_visits: dict[tuple, dict] = defaultdict(dict)
        self._unit_visit_count: dict[tuple, int] = {}

        for _, row in visit_map.iterrows():
            key = (row["etld"], row["location"])
            self._unit_visits[key][row["access_id"]] = row["session_id"]
            self._unit_visit_count[key] = row["visit_count"]

    def visit_count(self, etld: str, location: str) -> int:
        return self._unit_visit_count.get((etld, location), 0)

    def all_session_ids(self, etld: str, location: str) -> set[int]:
        return set(self._unit_visits.get((etld, location), {}).values())

    def sufficient_visits(self, etld: str, location: str) -> bool:
        return self.visit_count(etld, location) >= MIN_VISITS

    # ── Binary presence ───────────────────────────────────────────────────────

    def tag_binary(self, etld: str, location: str,
                   observed_session_ids: set[int]) -> str:
        """
        Tag a binary signal (present/absent per session).

        Args:
            observed_session_ids: set of session_ids in which the signal was observed.

        Returns: TIER_A if present in all visits, TIER_B if present in ≥1 visit,
                 TIER_ABSENT if never observed.
        """
        key = (etld, location)
        all_sessions = self.all_session_ids(etld, location)
        if not all_sessions or not observed_session_ids:
            return TIER_ABSENT if not observed_session_ids else TIER_B

        observed = observed_session_ids & all_sessions
        if not observed:
            return TIER_ABSENT

        n_visits = self.visit_count(etld, location)
        n_observed = len({
            access_id
            for access_id, sid in self._unit_visits[key].items()
            if sid in observed
        })

        if n_observed >= n_visits and n_visits >= MIN_VISITS:
            return TIER_A
        return TIER_B

    # ── Valued signal ─────────────────────────────────────────────────────────

    def tag_value(self, etld: str, location: str,
                  session_values: dict[int, str],
                  normalise_fn: Callable[[str], str] = normalise_noop) -> tuple[str, str]:
        """
        Tag a valued signal (e.g. header value, script src).

        Args:
            session_values: {session_id: value_string}
            normalise_fn:   normalisation function applied before comparison

        Returns: (tier, resolved_value) where resolved_value is the
                 normalised value (for Tier A) or most common value (for Tier B).
        """
        key = (etld, location)
        all_sessions = self.all_session_ids(etld, location)
        present = {sid: v for sid, v in session_values.items()
                   if sid in all_sessions and v is not None and v != ""}
        if not present:
            return TIER_ABSENT, ""

        norm_values = {sid: normalise_fn(v) for sid, v in present.items()}
        unique_norm = set(norm_values.values())

        n_visits = self.visit_count(etld, location)
        n_present = len({
            access_id
            for access_id, sid in self._unit_visits[key].items()
            if sid in present
        })

        if n_present >= n_visits and len(unique_norm) == 1 and n_visits >= MIN_VISITS:
            return TIER_A, next(iter(unique_norm))

        # Tier B: return most common normalised value
        from collections import Counter
        most_common = Counter(norm_values.values()).most_common(1)[0][0]
        return TIER_B, most_common

    # ── JS call tagging ───────────────────────────────────────────────────────

    def tag_js_call(self, etld: str, location: str,
                    observed_session_ids: set[int],
                    api_category: str) -> str:
        """
        Tag a JS call signal, applying category-specific tier rules.

        For JS_TIER_A_CATEGORIES: Tier A requires presence in all visits.
        For JS_TIER_B_PRIMARY_CATEGORIES: Tier B is the primary tier;
            Tier A is still computed and returned if consistent, but the
            caller should treat Tier B as the main reporting tier.
        """
        return self.tag_binary(etld, location, observed_session_ids)

    def js_primary_tier(self, api_category: str) -> str:
        """
        Returns which tier is the 'primary' reporting tier for a given
        JS API category. Callers use this to decide which tier goes into
        the main results table vs the supplementary one.
        """
        if api_category in JS_TIER_A_CATEGORIES:
            return TIER_A
        return TIER_B

    # ── Batch tagging ─────────────────────────────────────────────────────────

    def tag_signal_series(self, df: pd.DataFrame,
                          session_col: str = "session_id",
                          etld_col: str = "page_etld",
                          loc_col: str = "location") -> pd.Series:
        """
        For a DataFrame where each row is one observation of a signal in one session,
        group by (etld, location) and return a Series of tier labels,
        one per unique (etld, location) pair.

        Useful for binary signals (script present, header present, etc.).
        Returns a DataFrame with (etld, location, tier) columns.
        """
        rows = []
        for (etld, loc), grp in df.groupby([etld_col, loc_col]):
            observed = set(grp[session_col].unique())
            tier = self.tag_binary(etld, loc, observed)
            rows.append({etld_col: etld, loc_col: loc, "tier": tier})
        return pd.DataFrame(rows)


# ── Consistency summary helpers ───────────────────────────────────────────────

def summarise_tiers(tier_series: pd.Series) -> dict:
    """
    Given a Series of tier labels, return counts and percentages.
    """
    from collections import Counter
    counts = Counter(tier_series)
    total = len(tier_series)
    return {
        "total":       total,
        "stable":      counts.get(TIER_A, 0),
        "meaningful":  counts.get(TIER_B, 0),
        "absent":      counts.get(TIER_ABSENT, 0),
        "stable_pct":     counts.get(TIER_A, 0) / total * 100 if total else 0,
        "meaningful_pct": counts.get(TIER_B, 0) / total * 100 if total else 0,
    }


def add_tier_column(df: pd.DataFrame, tagger: "ConsistencyTagger",
                    session_col: str = "session_id",
                    etld_col: str = "page_etld",
                    loc_col: str = "location",
                    tier_col: str = "consistency_tier") -> pd.DataFrame:
    """
    Convenience: given a long-format DataFrame (one row per session × signal),
    add a `consistency_tier` column reflecting the per-etld×location tier.

    Works for binary signals where presence = the row existing.
    Returns df with tier_col added.
    """
    tier_map = (
        tagger.tag_signal_series(df, session_col, etld_col, loc_col)
        .set_index([etld_col, loc_col])["tier"]
    )
    df = df.copy()
    df[tier_col] = df.apply(
        lambda r: tier_map.get((r[etld_col], r[loc_col]), TIER_ABSENT), axis=1
    )
    return df


# ── Visit sufficiency filter ──────────────────────────────────────────────────

def filter_sufficient_visits(df: pd.DataFrame,
                              visit_map: pd.DataFrame,
                              etld_col: str = "page_etld",
                              loc_col: str = "location") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split a DataFrame into (sufficient, insufficient) based on whether the
    etld × location pair has ≥ MIN_VISITS visits.

    Returns (sufficient_df, excluded_df).
    Sufficient df is the one to use for Tier A/B analysis.
    Excluded df is reported separately as data quality note.
    """
    visit_counts = (visit_map.groupby(["etld", "location"])["access_id"]
                              .nunique()
                              .reset_index(name="visit_count"))
    visit_counts = visit_counts.rename(columns={"etld": etld_col,
                                                 "location": loc_col})
    merged = df.merge(visit_counts, on=[etld_col, loc_col], how="left")
    merged["visit_count"] = merged["visit_count"].fillna(0).astype(int)

    sufficient  = merged[merged["visit_count"] >= MIN_VISITS].drop(
        columns=["visit_count"])
    insufficient= merged[merged["visit_count"] < MIN_VISITS].drop(
        columns=["visit_count"])
    return sufficient, insufficient
