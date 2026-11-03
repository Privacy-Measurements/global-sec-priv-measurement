# VPN cross-country analysis — scripts

Scripts that reproduce three VPN vs. non-VPN cross-country figures:

- `results/vpn/cross_country/iframe_origin_delta_heatmap.png` — `src/vpn/iframe_analysis.py`
- `results/vpn/cross_country/script_domain_delta_cross_country.png` — `src/vpn/script_analysis.py`
- `results/vpn/cross_country/js_category_delta_tp.png` — `src/vpn/js_call_analysis.py`

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your own DB credentials
```

Requires a live MySQL connection to a `crawl_analysis_new`-equivalent database
with the project's schema (`crawl_sessions`, `scripts`, `js_calls`, and the
`header_data_new` / `header_data_vpn` tables). This bundle is scripts-only —
it is **not standalone-runnable** without that database access and schema.

## Running

From this directory's root (each script does its own `sys.path` setup, so it
expects to be invoked relative to this root):

```bash
python -m src.vpn.iframe_analysis
python -m src.vpn.script_analysis
python -m src.vpn.js_call_analysis
```

Each script writes per-country outputs under `results/vpn/<country>/` and the
cross-country figures under `results/vpn/cross_country/`, matching the layout
of the figures listed above.

## Layout notes

- `src/vpn/vpn_shared.py` is a trimmed extract of the project's internal
  `src/vpn/utils.py`, containing only the functions these three scripts use.
- `src/scripts/domain_utils.py` is a one-function extract (`_registered_domain`)
  from the project's internal `src/scripts/inclusion_chain.py`.
- `src/headers/config.py` loads `SECURITY_HEADERS.json` / `PERMISSIONS_POLICY.json`
  via a path relative to the current working directory, so scripts must be run
  from this directory's root (not from inside `src/`).
