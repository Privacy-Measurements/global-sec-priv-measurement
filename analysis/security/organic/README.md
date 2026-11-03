# Non-VPN header/script table generation — scripts

Scripts that reproduce four LaTeX tables:

- `tables/headers_filled.tex` — security header adoption rates
- `tables/third-party_scripts_filled.tex` — third-party script prevalence
- `tables/security_lottery_filled.tex` — Security Lottery classification (Roth et al., USENIX Sec 2022)
- `tables/vpn_headers.tex` — **static artifact, no generating script** (see below)

## Pipeline

Each table is produced in two stages:

1. An upstream analysis script queries the database and writes CSV/JSON
   results:
   - `src/headers/header_deployment.py` → `results/headers/header_deployment/<category>/*.csv`
   - `src/headers/security_lottery.py` → `results/headers/security_lottery/<category>/*.json`
   - `src/scripts/script_analysis.py` → `results/scripts/<category>/tp_source_table.csv`
2. A `fill_*_table.py` script reads those results plus a hand-written LaTeX
   template and writes the final `_filled.tex`:
   - `src/analysis/fill_headers_table.py` → `tables/headers.tex` → `tables/headers_filled.tex`
   - `src/analysis/fill_scripts_table.py` → `tables/third-party_scripts.tex` → `tables/third-party_scripts_filled.tex`
   - `src/analysis/fill_security_lottery_table.py` → `tables/security_lottery.tex` → `tables/security_lottery_filled.tex`

`<category>` is one of `global`, `country_coded`, `country_specific`.

### `tables/vpn_headers.tex`

This table has no reproducing script in the codebase — it is a hand-finalized
artifact. To automate it, script logic would need to be written to emit this
exact table shape (per-header, per-country VPN vs. physical-access adoption
rates) from the VPN header analysis pipeline; that script does not currently
exist.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your own DB credentials
```

Requires a live MySQL connection to a `crawl_analysis_last`-equivalent
database with the project's schema (`crawl_sessions`, `har_requests`,
`cookies`, `scripts` tables). This bundle is scripts-only — it is **not
standalone-runnable** without that database access and schema, and without
first running the upstream analysis scripts to populate `results/`.

## Running

From this directory's root:

```bash
python src/headers/header_deployment.py
python src/headers/security_lottery.py
python src/scripts/script_analysis.py

python src/analysis/fill_headers_table.py
python src/analysis/fill_scripts_table.py
python src/analysis/fill_security_lottery_table.py
```

## Layout notes

- `src/headers/source_profiler_lists.py` is a three-function extract
  (`_load_disconnect`, `_load_tracker_set`, `_load_fingerprinting_domains`)
  from the project's internal `src/headers/source_profiler.py`, used by
  `src/scripts/script_analysis.py` for tracker/fingerprinting-domain lookups.
- `privacy-lists/` contains only the four list files those three functions
  read (`disconnect.json`, `easylist_domains.txt`, `consolidated_origins.json`,
  `umar_iqbal_fingerprinting_list.json`).
- `src/scripts/domain_categoriser.py` calls the Cloudflare Radar API when
  `CF_RADAR_API` is set; without it, domain categorisation falls back to
  pattern/TLD heuristics only.
