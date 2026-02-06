# web-priv-measurements

## URL Filter Logic:

We plan to collect website data for this study by visiting Top `N` eTLD+1 domains from three different categories/buckets:
- _First Bucket_: Top `N` **globally popular** domains (e.g., `netflix.com`).
- _Second Bucket_: Top `N` domains that have corresponding eTLDs for individual countries (e.g., `amazon.com` -> `amazon.ae`)
- _Third Bucket_: Top `N` domains that:
    - do not have a registered domain name, in the list of **globally popular** domains (subsequently, also ruling out the second bucket) (e.g., all `amazon` domains will be filtered out since `amazon.com` exists, while `rentitonline.ae` is accepted since `rentitonline` domain does not exist in the first bucket.)
    - has the corresponding country code of individual country in its eTLD/suffix (e.g., `UK`: `gov.uk` -> **Accepted!**)

The [crux_urls/filtered_urls.zip](crux_urls/filtered_urls.zip) file contains the URLs segregated in the three buckets as described above. All the CRuX URLs (global as well as country specific URLs) were downloaded, based on their popularity ranking on 31<sup>st</sup> January, 2025, The unzipped directory contains the following files:
- `global_bucket.json`: It contains the **globally popular** CRuX URLs, corresponding to the first category/bucket of URLs we use for our study.
- `mid_bucket.json`: It contains the local eTLD+1 counterparts for the **globally popular** CRuX URLs, corresponding to the second category/bucket of URLs we use for our study.
- `country_only_bucket.json`: This is an intermediary list of URLs, that **only satisfies the first pre-condiiton** to qualify for the country-specific or the third bucket of URLs.
- `filtered_country_only_bucket.json`: It contains the list of URLs, that **satisfies all the pre-condiiton** to qualify for the country-specific or the third bucket of URLs.

Note: A previous commit (`35735ecd48caa5e003d7bd5f3d2e0f5a8d11ddd6`) also includes the raw list of all the extracted URLs, but had to be removed for space limitation reasons.