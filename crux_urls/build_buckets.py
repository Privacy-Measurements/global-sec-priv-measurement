import pandas as pd
import tldextract
import json

global_input_file = "crux_raw_urls/GLOBAL-top-million-20250818.json"
global_output_file = "buckets/D1-global.json"
d2_output_file = "buckets/D2-country_coded.json"
d3_output_file = "buckets/D3-country_specific.json"

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
    return 
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
        json.dump(d3_buckets, fh, indent=4, default=serialize_object)



if __name__ == "__main__":
    print('Building Global Bucket: ')
    print('****************************************')
    build_global_bucket()
    print('****************************************')
    print('Building D2 Bucket: ')
    print('****************************************')
  #  build_d2_buckets()
    print('****************************************')
    print('Building D3 Bucket: ')
    print('****************************************')
  #  build_d3_buckets()

