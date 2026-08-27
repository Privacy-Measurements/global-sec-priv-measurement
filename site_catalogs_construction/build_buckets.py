import pandas as pd
import tldextract
import json
from collections import defaultdict
from pathlib import Path

global_input_file = "crux_raw_urls/GLOBAL-top-million-20250818.json"
global_output_file = "catalogs/buckets/D1-global.json"
d2_output_file = "catalogs/buckets/D2-country_coded.json"
d2_to_global_output_file = "catalogs/D2_to_global/D2_to_GLOBAL.csv"
d2_not_in_d1_output_file = "catalogs/D2_to_global/D2_equivalents_not_in_D1.csv"
d3_output_file = "catalogs/buckets/D3-country_specific.json"

country_input_files = {
    "in": "crux_raw_urls/INDIA-top-million-20250818.json",
    "de": "crux_raw_urls/GERMANY-top-million-20250818.json",
    "ae": "crux_raw_urls/UAE-top-million-20250818.json",
    "us": "crux_raw_urls/USA-top-million-20250818.json",
    "dz": "crux_raw_urls/ALGERIA-top-million-20250818.json",
}

vantage_tlds = {
    "in": ['in'],
    'de': ['de', 'bayern', 'berlin', 'hamburg', 'nrw', 'saarland', 'cologne', 'koeln'],
    'us': ['us', 'boston', 'miami', 'nyc'],
    'in': ['in'],
    'dz': ['dz'],
    'ae': ['ae', 'abudhabi', 'dubai']
}

TARGET_TOP = 10000 # Number of sites to put in each bucket
GLOBAL_BACKUP_TOP = 50000 # The rank of sites to use to fill the D1 bucket once we apply the filter 


def serialize_object(obj: object) -> object:
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, defaultdict):
        return dict(obj)
    else:
        raise TypeError(f"Type {type(obj)} not serializable")

def write_df_to_json(df, file_name):
    Path(file_name).parent.mkdir(parents=True, exist_ok=True)
    with open(file_name, "w", encoding="utf-8") as fh:
        json.dump(
            df.to_dict(orient="records"),  
            fh,
            indent=4,
            default=serialize_object,      
            ensure_ascii=False             
        )

def load_json_to_df(file_path): 
    """
    Read a crux json file into a DataFrame    
    """

    with open(file_path, "r") as f:
        df = pd.read_json(f, lines=True)

    df["rank"] = df["rank"].astype(int)  
    return df

def is_vantage_suffix(input_suffix, country=None):
    """
    Return True if the input suffix is country specific.    

    If country is given, consider only the suffixes of that country.
    If country is not given, check suffixes of all countries.
    """
    
    if country:
        suffixes = vantage_tlds.get(country.lower())
        return any(input_suffix == vt or input_suffix.endswith("." + vt) for vt in suffixes)
    
    # Check all countries
    for suffixes in vantage_tlds.values():
        if any(input_suffix == vt or input_suffix.endswith("." + vt) for vt in suffixes):
            return True
            
    return False

def get_unique_origins(df, rank):
    """
    Given a DataFrame with columns ['origin','rank'], restricts to rank <= rank,
    extracts fqdns, and ensures their uniqueness by keeping the lowest rank per fqdn.
    Returns DataFrame with ['origin','rank', 'fqdn', 'domain', 'suffix', 'fqdn-suffix']
    """

    df = df[df["rank"] <= rank].copy()
    print(f"After restricting to rank <= {rank}: {len(df)} rows")

    # Extract FQDNs
    df["fqdn"] = df["origin"].apply(lambda x: tldextract.extract(x).fqdn)
    df["domain"] = df["origin"].apply(lambda x: tldextract.extract(x).domain)
    df["suffix"] = df["origin"].apply(lambda x: tldextract.extract(x).suffix)
    df["fqdn-suffix"] = df.apply(
        lambda row: row["fqdn"][: -(len(row["suffix"]) + 1)]
        if row["suffix"] else row["fqdn"],
        axis=1
    )
    # Keep only the row with the lowest rank per fqdn
    df = df.sort_values(["rank", "origin"])
    idx = df.groupby("fqdn")["rank"].idxmin()
    df_unique = df.loc[idx].copy()

    return df_unique.sort_values("rank").reset_index(drop=True)

def build_global_bucket():
    # Read input
    df = load_json_to_df(global_input_file)
    print(f"Loaded input file with {len(df)} rows")

    # Step 1: Get unique Fqdns
    df_target = get_unique_origins(df, TARGET_TOP)
    print(f"Number of unique fqdns: {len(df_target)}")

    # Step 2: Apply the country-coded filter
    df_target = df_target[~df_target["suffix"].apply(is_vantage_suffix)].copy()   
    print(f"Number of fqdn after removing country-coded sites: {len(df_target)}")


    if len(df_target) < TARGET_TOP:

        print('---------------')
        print('Completing the list with Top 50k')

        # Step 1: Get the back up fqdns
        df_backup = get_unique_origins(df, GLOBAL_BACKUP_TOP)
        print(f"Number of unique fqdns: {len(df_backup)}")

        # Step 2: Remove fqdn that were already found in the top 10k
        df_backup = df_backup[~df_backup["fqdn"].isin(df_target["fqdn"])]
        print(f"Number of fqdn after removing fqdns already encountered in Top 10k: {len(df_backup)}")


        
        # Step 3: Apply the country-coded filter
        df_backup = df_backup[~df_backup["suffix"].apply(is_vantage_suffix)].copy()                
        print(f"Number of fqdn after removing country-coded sites: {len(df_backup)}")

        # Fill missing if needed
        missing = TARGET_TOP - len(df_target)


        # Pick missing ones
        extra = df_backup.head(missing)
        df_target = pd.concat([df_target, extra]).reset_index(drop=True)

    # Final size
    print(f"D1 bucket created with {len(df_target)} origins")


    write_df_to_json(df_target, global_output_file)
    return df_target

def get_country_d2(country_code, country_file, global_fqdns_without_suffixes):
    """Build D2 for a single country."""

    # Step 1: eading country's Crux file
    df_country = load_json_to_df(country_file)
    
    # Step 2: Get unique FQDNs
    df_country = get_unique_origins(df_country, 1000000)

    # Step 3: keep only rows with the one of the country's suffixes
    df_country = df_country[df_country["suffix"].apply(lambda x: is_vantage_suffix(x, country_code))].copy()

    # Step 4: keep only rows where fqdn-suffix appears in global_fqdns_without_suffixes
    df_country = df_country[df_country["fqdn-suffix"].isin(global_fqdns_without_suffixes)].copy()


    # Step 5: Order by rank
    df_country = df_country.sort_values(by="rank", ascending=True)

    # Step 6: Keep first 10k elements
    df_country = df_country.head(TARGET_TOP).reset_index(drop=True)

    return df_country

def build_d2_buckets():
    """Build D2 for all countries."""

    d2_buckets = {}


    #Iterate over countries 
    for country_code, country_file in country_input_files.items():
        
        print('------------------')
        print(f"Building D2 for {country_code}")

        df_global = load_json_to_df(global_input_file)
        df_global = get_unique_origins(df_global, 1000000)
        df_global = df_global[~df_global["suffix"].apply(lambda x: is_vantage_suffix(x, country_code))].copy()
        global_fqdn_without_suffix = set(df_global["fqdn-suffix"])

        print(f"Total distinct fqdn_without_suffix in the global crux: {len(global_fqdn_without_suffix)}")

        # Building country's D2
        df_d2 = get_country_d2(country_code, country_file, global_fqdn_without_suffix)

        # Store as list of dicts
        d2_buckets[country_code] = df_d2.to_dict(orient="records")
        print(f"{country_code.upper()} D2: {len(df_d2)} origins")

    
    with open(d2_output_file, 'w') as fh:
        Path(d2_output_file).parent.mkdir(parents=True, exist_ok=True)
        json.dump(d2_buckets, fh, indent=4, default=serialize_object)

def get_country_d3(country_code, country_file, d2_fqdn_for_country):
    """Build D3 for a single country."""

    # Step 1: eading country's Crux file
    df_country = load_json_to_df(country_file)
    print(f"Loaded {len(df_country)} rows from file")

    # Step 2: Get unique FQDNs
    df_country = get_unique_origins(df_country, 1000000)
    print(f"Number of unique FQDNs: {len(df_country)}")

    # Step 3: keep only rows with the one of the country's suffixes
    df_country = df_country[df_country["suffix"].apply(lambda x: is_vantage_suffix(x, country_code))].copy()
    print(f"Number of rows after country-suffix filtering: {len(df_country)}")

    # Step 4: Exclude rows where FQDN appears in D2 
    df_country = df_country[~df_country["fqdn"].isin(d2_fqdn_for_country)].copy()

    # Step 5: Order by rank
    df_country = df_country.sort_values(by="rank", ascending=True)

    # Step 6: Keep first 10k elements
    df_country = df_country.head(TARGET_TOP).reset_index(drop=True)

    return df_country



def build_d3_buckets():
    """Build D3 buckets for all countries."""

    d3_buckets = {}

    # Read d2
    with open(d2_output_file, "r") as f:
        d2_data = json.load(f)
    

    for country_code, country_file in country_input_files.items():
        print('------------------')
        print(f"Building D3 for {country_code}")

        #Get the country's D2 list of the country, to use as a filter 

        d2_fqdn_for_country = set([item['fqdn'] for item in d2_data[country_code]])

        df_d3 = get_country_d3(country_code, country_file, d2_fqdn_for_country)

        d3_buckets[country_code] = df_d3.to_dict(orient="records")

        print(f"{country_code.upper()} D3: {len(df_d3)} origins")


    with open(d3_output_file, 'w') as fh:
        Path(d3_output_file).parent.mkdir(parents=True, exist_ok=True)
        json.dump(d3_buckets, fh, indent=4, default=serialize_object)





def build_d2_to_global_mapping(df_d1):
    """
    For each D2 site, find its global equivalents (same fqdn-suffix, no country TLD).
    Saves two files:
      - D2_to_GLOBAL.csv:             country, D2_site, equivalents (all global matches)
            - D2_equivalents_not_in_D1.csv: site (distinct equivalents that are not in D1)
    """
    d1_fqdns = set(df_d1["fqdn"])

    with open(d2_output_file, "r") as f:
        d2_data = json.load(f)

    rows_all = []
    not_in_d1_sites = set()

    for country_code in country_input_files:
        print(f"Mapping D2 equivalents for {country_code}")

        # Build global index without this country's suffixes
        df_global = load_json_to_df(global_input_file)
        df_global = get_unique_origins(df_global, 1000000)
        df_global = df_global[~df_global["suffix"].apply(lambda x: is_vantage_suffix(x, country_code))].copy()

        # fqdn-suffix -> list of global fqdns that share the same base
        suffix_to_global = df_global.groupby("fqdn-suffix")["fqdn"].apply(list).to_dict()

        for item in d2_data[country_code]:
            d2_site = item["fqdn"]
            fqdn_suffix = item["fqdn-suffix"]
            equivalents = suffix_to_global.get(fqdn_suffix, [])

            rows_all.append({
                "country": country_code,
                "D2_site": d2_site,
                "equivalents": equivalents,
            })

            not_in_d1 = [eq for eq in equivalents if eq not in d1_fqdns]
            not_in_d1_sites.update(not_in_d1)

    Path(d2_to_global_output_file).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_all).to_csv(d2_to_global_output_file, index=False)
    print(f"Saved {d2_to_global_output_file}: {len(rows_all)} rows")

    Path(d2_not_in_d1_output_file).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"site": sorted(not_in_d1_sites)}).to_csv(d2_not_in_d1_output_file, index=False)
    print(f"Saved {d2_not_in_d1_output_file}: {len(not_in_d1_sites)} rows")

if __name__ == "__main__":
    print('Building Global Bucket: ')
    df_d1 = build_global_bucket()
    print('Building D2 Bucket: ')
    build_d2_buckets()
    print('Building D3 Bucket: ')
    build_d3_buckets()
    print('Building D2 to Global mapping: ')
    build_d2_to_global_mapping(df_d1)

