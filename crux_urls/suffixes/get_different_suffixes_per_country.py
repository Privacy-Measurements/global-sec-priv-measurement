import pandas as pd
import tldextract
import json
import re
import socket
import os
import time


target_countries = ["India", "United States of America (the)", "United Arab Emirates (the)", "Germany", "Algeria"]


public_suffix_info_file = "public_suffix_whois_info.json"

country_input_files = {
    "in": "../crux_raw_urls/INDIA-top-million-20250818.json",
    "de": "../crux_raw_urls/GERMANY-top-million-20250818.json",
    "ae": "../crux_raw_urls/UAE-top-million-20250818.json",
#    "us": "../crux_raw_urls/USA-top-million-20250818.json",
    "dz": "../crux_raw_urls/ALGERIA-top-million-20250818.json",
}



def load_json_to_df(file_path):
    df = pd.read_json(file_path, lines=True)
    return df

def get_unique_public_suffixes(df):

    df["suffix"] = df["origin"].apply(lambda x: tldextract.extract(x).suffix)
    unique_suffixes = df["suffix"].dropna().unique()
    print("Distinct public suffixes:", len(unique_suffixes))

    return sorted(unique_suffixes)

def parse_iana_whois(raw_text):
    """
    Parse raw IANA WHOIS response into a structured dictionary.
    Handles multiple contacts and multi-line fields like address.
    """
    data = {}
    current_contact = None

    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith('%'):
            continue 

        contact_match = re.match(r'contact:\s*(\w+)', line)
        if contact_match:
            current_contact = contact_match.group(1).lower()
            if 'contacts' not in data:
                data['contacts'] = {}
            data['contacts'][current_contact] = {}
            continue

        key_val_match = re.match(r'([\w\-\s]+):\s*(.*)', line)
        if key_val_match:
            key = key_val_match.group(1).strip().lower().replace(' ', '_').replace('-', '_')
            value = key_val_match.group(2).strip()

            if current_contact:
                if key == 'address':
                    data['contacts'][current_contact].setdefault('address', []).append(value)
                else:
                    data['contacts'][current_contact][key] = value
            else:
                if key in ['nserver', 'ds_rdata', 'remarks', 'address']:
                    data.setdefault(key, []).append(value)
                else:
                    data[key] = value

    return data

def query_iana(tld):
    server = 'whois.iana.org'
    port = 43
    response = ""

    tld = tld.lstrip(".")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server, port))
        s.sendall((tld + "\r\n").encode())
        
        while True:
            data = s.recv(4096)
            if not data:
                break
            response += data.decode()

    return response

def get_whois_info():

    all_dfs = []
    for country_code, file in country_input_files.items():
        df_country = load_json_to_df(file)
        all_dfs.append(df_country)

    merged_df = pd.concat(all_dfs, ignore_index=True)
    public_suffixes = get_unique_public_suffixes(merged_df)


    res = {}
    batch_size = 100
    retry_delay = 5  # seconds
    next_one_delay = 2


    for i, public_suffix in enumerate(public_suffixes, start=1):
        attempts = 0
        while attempts < 3:
            try:
                raw = query_iana(public_suffix)
                parsed = parse_iana_whois(raw)
                res[public_suffix] = parsed
                break
            except Exception as e:
                attempts += 1
                error = e
                time.sleep(retry_delay)

        else:
            print(f'Error for {public_suffix}: {e}')

        time.sleep(next_one_delay)

    with open(public_suffix_info_file, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2, ensure_ascii=False)


def group_suffixes_per_etld(suffix_info):

    country_groups = {}

    for suffix, info in suffix_info.items():
        address = info.get("address", [])
        if address:
            country = address[-1].strip()
        else:
            country = "UNKNOWN"


        country_groups.setdefault(country, []).append(suffix)


    for tc in target_countries + ['UNKNOWN']:
        matches = country_groups.get(tc, [])
        print(f"\nSuffixes for {tc} ({len(matches)}):")
        print(", ".join(sorted(matches)))

    return country_groups

def count_suffix_usage(grouped_suffixes):
    """
    For each country, for each suffix belonging to that country,
    count how many domains from that country's top-million dataset 
    use that suffix, grouped by rank.

    Returns:
        usage_data: dict
            {
              "Country Name": {
                  "suffix1": Series(rank -> count),
                  "suffix2": Series(rank -> count),
                  ...
              },
              ...
            }
    """
    usage_data = {}

    # map ISO code in country_input_files to full country name
    code_to_country = { 
        "in": "India",
        "us": "United States of America (the)",
        "ae": "United Arab Emirates (the)",
        "de": "Germany",
        "dz": "Algeria"
    }

    for cc, file in country_input_files.items():
        country = code_to_country.get(cc, "UNKNOWN")
        suffixes = grouped_suffixes.get(country, [])

        if not suffixes:
            continue

        if not os.path.exists(file):
            print(f"[WARN] Missing file for {cc}: {file}")
            continue

        df = load_json_to_df(file)
        df["suffix"] = df["origin"].apply(lambda x: tldextract.extract(x).suffix)

        if "rank" not in df.columns:
            print(f"[WARN] No rank column in {file}, skipping.")
            continue

        usage_data[country] = {}
        print(f"\n=== Usage for {country} ===")

        for suffix in suffixes:
            df_suffix = df[df["suffix"] == suffix]

            if df_suffix.empty:
                continue

            grouped = df_suffix.groupby("rank").size()
            usage_data[country][suffix] = grouped

            print(f"  {suffix}: {len(df_suffix)} sites")
            print(grouped.to_string())

    return usage_data

if __name__ == "__main__":

    if not os.path.exists(public_suffix_info_file):
        get_whois_info()

    print('Whois data found')


    with open(public_suffix_info_file, "r", encoding="utf-8") as f:
        suffix_info = json.load(f)


    country_groups = group_suffixes_per_etld(suffix_info)

    count_suffix_usage(country_groups)