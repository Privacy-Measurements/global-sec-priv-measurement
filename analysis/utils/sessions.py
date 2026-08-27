GLOBAL_LOCATIONS = {"ALGERIA", "GERMANY", "INDIA", "UAE", "USA"}


# Keep only global sites that exist in all five countries.
def filter_global_to_common_etlds(sessions, site_tracking):
    # Get unique (site, country) pairs for global rows only.
    global_pairs = (
        sessions[sessions["category"] == "global"][["etld", "location"]]
        .drop_duplicates()
    )

    # Build a location set per site and keep only sites present in all countries.
    etld_locations = global_pairs.groupby("etld")["location"].agg(lambda vals: set(vals))
    common_global_etlds = {
        etld
        for etld, locations in etld_locations.items()
        if locations == GLOBAL_LOCATIONS
    }

    # Keep all non-global rows, and keep only common-pool sites for global rows.
    sessions_filtered = sessions[
        (sessions["category"] != "global")
        | (sessions["etld"].isin(common_global_etlds))
    ].copy()

    # Apply the same global-site filter to the tracker presence table.
    site_tracking_filtered = site_tracking[
        (site_tracking["category"] != "global")
        | (site_tracking["etld"].isin(common_global_etlds))
    ].copy()

    # Return filtered sessions and filtered site-tracking rows.
    return sessions_filtered, site_tracking_filtered
