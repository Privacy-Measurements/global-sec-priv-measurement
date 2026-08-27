#!/usr/bin/env python3

import itertools
from pathlib import Path

import pandas as pd

CSV_PATH = Path(__file__).resolve().parent / "reports_D1" / "etld_tracker_sets.csv"
OUTPUT_PATH = Path(__file__).resolve().parent / "reports_D1" / "country_pair_tracker_count_comparison.csv"


def load_tracker_table(csv_path):
    df = pd.read_csv(csv_path)
    if df.empty:
        return df

    count_cols = [col for col in df.columns if col.startswith("n_trackers_")]
    if not count_cols:
        raise ValueError(f"No tracker count columns found in {csv_path}")

    return df[["etld"] + count_cols].copy()


def compare_country_pairs(df):
    count_cols = [col for col in df.columns if col.startswith("n_trackers_")]
    countries = [col.replace("n_trackers_", "") for col in count_cols]

    rows = []
    for country_a, country_b in itertools.combinations(countries, 2):
        col_a = f"n_trackers_{country_a}"
        col_b = f"n_trackers_{country_b}"

        pair_df = df[["etld", col_a, col_b]].dropna().copy()
        if pair_df.empty:
            rows.append({
                "country_a": country_a,
                "country_b": country_b,
                "n_sites": 0,
                "pct_more_in_a": 0.0,
                "pct_more_in_b": 0.0,
                "pct_equal": 0.0,
                "n_more_in_a": 0,
                "n_more_in_b": 0,
                "n_equal": 0,
            })
            continue

        pair_df["diff"] = pair_df[col_a].astype(int) - pair_df[col_b].astype(int)

        n_more_in_a = int((pair_df["diff"] > 0).sum())
        n_more_in_b = int((pair_df["diff"] < 0).sum())
        n_equal = int((pair_df["diff"] == 0).sum())
        n_sites = len(pair_df)

        rows.append({
            "country_a": country_a,
            "country_b": country_b,
            "pct_more_in_a": round((n_more_in_a / n_sites) * 100, 2) if n_sites else 0.0,
            "pct_more_in_b": round((n_more_in_b / n_sites) * 100, 2) if n_sites else 0.0,
            "pct_equal": round((n_equal / n_sites) * 100, 2) if n_sites else 0.0,
        })

    return pd.DataFrame(rows).sort_values(["country_a", "country_b"]).reset_index(drop=True)


def main():
    df = load_tracker_table(CSV_PATH)
    comparison = compare_country_pairs(df)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(OUTPUT_PATH, index=False)

    print(comparison.to_string(index=False))
    print(f"\nSaved comparison table to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
