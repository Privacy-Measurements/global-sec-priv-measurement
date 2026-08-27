#!/usr/bin/env python3

import os
from pathlib import Path

import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = CURRENT_DIR / "reports_D1"
GROUP_COMPARISONS_FILE = OUTPUT_DIR / "groups" / "all_group_comparisons.csv"
TOP_N = 5


def _split_group_names(value):
    text = str(value).strip()
    if not text or text == "None":
        return ()
    if "|" in text:
        return tuple(sorted(part for part in text.split("|") if part))
    return (text,)


def canonical_pair(group_a: str, group_b: str):
    left = _split_group_names(group_a)
    right = _split_group_names(group_b)
    return tuple(sorted((left, right), key=lambda x: x))


def load_comparison_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Comparison file not found: {path}. Run d1_comparaison.py first."
        )

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Comparison file is empty: {path}")

    df = df.copy()
    df["metric_value"] = df["metric_value"].replace({"None": None, "nan": None})
    df["n_group_a_only"] = pd.to_numeric(df["n_group_a_only"], errors="coerce").fillna(0).astype(int)
    df["n_group_b_only"] = pd.to_numeric(df["n_group_b_only"], errors="coerce").fillna(0).astype(int)
    df["n_both"] = pd.to_numeric(df["n_both"], errors="coerce").fillna(0).astype(int)
    df["score"] = df["n_group_a_only"] + df["n_group_b_only"] - df["n_both"]
    df["pair_key"] = df.apply(lambda row: canonical_pair(row["group_a"], row["group_b"]), axis=1)
    return df


def keep_best_unordered_row(df):
    dedup = df.sort_values(["metric_name", "metric_value", "score", "group_a", "group_b"], ascending=[True, True, False, True, True])
    dedup = dedup.drop_duplicates(subset=["pair_key", "metric_name", "metric_value"], keep="first")
    return dedup.reset_index(drop=True)


def top_n_overall(df: pd.DataFrame, n: int = TOP_N) -> pd.DataFrame:
    rows = df[df["metric_name"] == "overall"].copy()
    if rows.empty:
        return pd.DataFrame(columns=[
            "group_a", "group_b", "metric_name", "metric_value",
            "n_group_a_only", "n_group_b_only", "n_both", "score"
        ])
    rows = rows.sort_values(["score", "n_group_a_only", "n_group_b_only"], ascending=[False, False, False])
    return rows.head(n).reset_index(drop=True)


def top_n_by_type(df: pd.DataFrame, n: int = TOP_N) -> pd.DataFrame:
    rows = df[df["metric_name"] == "fp_type"].copy()
    if rows.empty:
        return pd.DataFrame(columns=[
            "metric_value", "group_a", "group_b",
            "n_group_a_only", "n_group_b_only", "n_both", "score"
        ])

    rows = rows.sort_values(["metric_value", "score", "n_group_a_only", "n_group_b_only"], ascending=[True, False, False, False])
    top_rows = rows.groupby("metric_value", group_keys=False).head(n)
    return top_rows.reset_index(drop=True)


def print_summary_table(title: str, frame: pd.DataFrame, display_cols):
    print(f"\n{title}")
    print("-" * len(title))
    if frame.empty:
        print("No rows found.")
        return
    print(frame[display_cols].to_string(index=False))


def main():
    df = load_comparison_file(GROUP_COMPARISONS_FILE)
    df = keep_best_unordered_row(df)

    overall_top = top_n_overall(df, n=TOP_N)
    type_top = top_n_by_type(df, n=TOP_N)

    overall_out = OUTPUT_DIR / "d1_country_group_rankings_overall.csv"
    type_out = OUTPUT_DIR / "d1_country_group_rankings_by_type.csv"

    overall_top.to_csv(overall_out, index=False)
    type_top.to_csv(type_out, index=False)

    print_summary_table(
        f"Top {TOP_N} overall group differences",
        overall_top,
        ["group_a", "group_b", "n_group_a_only", "n_group_b_only", "n_both", "score"],
    )

    print_summary_table(
        f"Top {TOP_N} groups per fingerprinting type",
        type_top,
        ["metric_value", "group_a", "group_b", "n_group_a_only", "n_group_b_only", "n_both", "score"],
    )

    print(f"\nSaved overall rankings to: {overall_out}")
    print(f"Saved per-type rankings to: {type_out}")


if __name__ == "__main__":
    main()
