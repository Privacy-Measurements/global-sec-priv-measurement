from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
CASES_CSV = BASE_DIR / "reports_D1" / "chain_analysis" / "session_level_chain_deviation_cases.csv"
CHAINS_CSV = BASE_DIR / "reports_D1" / "chain_analysis" / "session_level_observed_inclusion_chains.csv"

STUDY_CASES = {
    "google_rum": {
        "label": "Google RUM (pagead2.googlesyndication.com/pagead/js/rum.js?fcd=true only)",
        "deviation_src_substr": "pagead2.googlesyndication.com/pagead/js/rum.js?fcd=true",
        "expected_tracker": "gstatic.com",
        "expected_direction": "country_present=GERMANY, country_missing=ALGERIA/INDIA/UAE/USA",
    },
    "yandex_metrika": {
        "label": "Yandex Metrika (mc.yandex.ru/metrika/tag.js)",
        "deviation_src_substr": "mc.yandex.ru/metrika/tag.js",
        "expected_tracker": "yandex.com",
        "expected_direction": "country_present=GERMANY or USA, country_missing=INDIA (primarily)",
    },
    "quantserve": {
        "label": "Quantserve/comScore (quantserve.com/quant.js)",
        "deviation_src_substr": "quantserve.com/quant.js",
        "expected_tracker": "quantserve.com",
        "expected_direction": "country_present=INDIA/UAE/USA, country_missing=GERMANY (2 Japanese news sites)",
    },
}


def load_cases():
    df = pd.read_csv(CASES_CSV)
    return df


def load_chains():
    return pd.read_csv(CHAINS_CSV)


def print_sql_for_sample(row, chains_df):
    sid = int(row["observed_session_id"])
    rid = int(row["observed_request_id"])
    etld = row["etld"]
    tracker = row["tracker_domain"]
    cp = row["country_present"]
    cm = row["country_missing"]

    print(f"\n  -- Sample: etld={etld}, {cp} -> {cm}")
    print(f"  -- observed_session_id={sid}, observed_request_id={rid}")
    print()

    # Step 1: verify the session exists with correct location and category
    print("  -- Step 1: verify session location and category")
    print(f"  SELECT id, etld, location, category FROM crawl_sessions")
    print(f"  WHERE id = {sid};")
    print()

    # Step 2: verify the tracker request
    print("  -- Step 2: verify the tracker request")
    print(f"  SELECT id, session_id, request_url, is_tracker, etld_request_url_rel, parent_id")
    print(f"  FROM requests")
    print(f"  WHERE session_id = {sid} AND id = '{rid}';")
    print()

    # Step 3: show the full recorded inclusion chain (from CSV, then SQL to confirm)
    chain_rows = chains_df[
        (chains_df["observed_session_id"] == sid)
        & (chains_df["observed_request_id"] == rid)
        & (chains_df["country_present"] == cp)
        & (chains_df["country_missing"] == cm)
    ].sort_values("depth")

    if not chain_rows.empty:
        print("  -- Step 3: inclusion chain as recorded in the CSV")
        for _, cr in chain_rows.iterrows():
            print(f"    depth={int(cr['depth'])}  node_id={cr['node_id']}  type={cr['node_type']}  "
                  f"parent_id={cr['parent_id']}  src={cr['src']}")
        print()

        # SQL to walk the chain for the first node
        first_node = chain_rows.iloc[0]
        node_id = first_node["node_id"]
        print("  -- Step 4: verify first chain node in DB (try scripts, then html_elements)")
        print(f"  SELECT session_id, script_id AS node_id, parent_id, script_src AS src")
        print(f"  FROM scripts")
        print(f"  WHERE session_id = {sid} AND script_id = '{node_id}';")
        print()
        print(f"  SELECT session_id, element_id AS node_id, parent_id, attrs_json")
        print(f"  FROM html_elements")
        print(f"  WHERE session_id = {sid} AND element_id = '{node_id}';")
    else:
        print("  -- Step 3: no chain rows found in CSV for this sample.")
    print()

    # Step 5: check the missing session has no matching request to that tracker
    missing_sid = int(row["missing_session_id"])
    print("  -- Step 5: confirm tracker is absent from the missing-country session")
    print(f"  SELECT id, request_url, is_tracker")
    print(f"  FROM requests")
    print(f"  WHERE session_id = {missing_sid}")
    print(f"    AND request_url LIKE '%{tracker}%'")
    print(f"    AND is_tracker = 1;")
    print(f"  -- (missing_session_id={missing_sid} is the {cm} session for etld={etld})")


def main():
    print("Loading CSVs...")
    cases_df = load_cases()
    chains_df = load_chains()

    for case_key, case_info in STUDY_CASES.items():
        print("\n" + "=" * 70)
        print(f"CASE: {case_info['label']}")
        print(f"Expected direction: {case_info['expected_direction']}")
        print("=" * 70)

        # Filter cases matching this deviation_src substring
        mask = cases_df["deviation_src"].str.contains(
            case_info["deviation_src_substr"], na=False, regex=False
        )
        matching = cases_df[mask].copy()

        if matching.empty:
            print("  *** No matching rows found in CSV — check deviation_src substring ***")
            continue

        # Summary stats
        n_etlds = matching["etld"].nunique()
        directions = (
            matching.groupby(["country_present", "country_missing"])
            .size()
            .reset_index(name="n_cases")
            .sort_values("n_cases", ascending=False)
        )
        print(f"\n  n_distinct_etlds      : {n_etlds}")
        print(f"  n_rows_in_csv         : {len(matching)}")
        print(f"  tracker_domains       : {sorted(matching['tracker_domain'].unique())}")
        print(f"\n  Directions (country_present -> country_missing):")
        for _, dr in directions.iterrows():
            print(f"    {dr['country_present']} -> {dr['country_missing']}  ({dr['n_cases']} cases)")

        # Show all etlds involved
        etld_list = sorted(matching["etld"].unique())
        print(f"\n  Etlds ({n_etlds} total):")
        for e in etld_list[:20]:
            print(f"    {e}")
        if n_etlds > 20:
            print(f"    ... and {n_etlds - 20} more")

        # Print SQL for one representative sample (most common etld)
        top_etld = matching["etld"].value_counts().index[0]
        sample_row = matching[matching["etld"] == top_etld].iloc[0]
        print(f"\n  SQL verification steps for sample etld: {top_etld}")
        print_sql_for_sample(sample_row, chains_df)


if __name__ == "__main__":
    main()
