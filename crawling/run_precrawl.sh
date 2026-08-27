#!/bin/bash
set -e


export BROWSER_FOR_PRECRAWL_PATH="resources/pagegraph_brave_build/Static/brave"

MAX_CORES=32 COUNTRY_FOR_PRECRAWL=dz npm run pre-crawl -- D2_to_global_global_sites_DZ.json
MAX_CORES=32 COUNTRY_FOR_PRECRAWL=ae npm run pre-crawl -- D2_to_global_global_sites_AE.json
MAX_CORES=32 COUNTRY_FOR_PRECRAWL=in npm run pre-crawl -- D2_to_global_global_sites_IN.json
MAX_CORES=32 COUNTRY_FOR_PRECRAWL=de npm run pre-crawl -- D2_to_global_global_sites_DE.json
MAX_CORES=32 COUNTRY_FOR_PRECRAWL=us npm run pre-crawl -- D2_to_global_global_sites_US.json
