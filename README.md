# Same Click, Different Risks: Geography as a Hidden Factor in Web Privacy and Security

This repository contains the artifacts for the paper "Same Click, Different Risks: Geography as a Hidden Factor in Web Privacy and Security". It contains the website catalogs for each country and the source code used to construct them, the measurement and crawling framework, the preprocessing pipeline, and the analysis scripts.


## Repository Layout

- `site_catalogs_construction/`: catalog construction, D2 catalog validation, and CRuX inputs.
- `crawling/`: crawling and PageGraph instrumentation.
- `pre_processing/`: GraphML processing, database creation, and feature extraction.
- `analysis/`: Comparative analyses of S&P measures.

## 1. Website Catalog Construction

Our study considers three catalogs of websites:  

- D1: globally popular domains.
- D2: country-coded counterparts of globally popular domains.
- D3: country-specific popular domains.

### Relevant code and assets

- `site_catalogs_construction/crux_raw_urls/`: raw CRuX input dumps extracted for each country and the global dataset. 
- `site_catalogs_construction/D2_catalog_validation/`: validation for D2 sites using favicon similarity and TLS records.
- `site_catalogs_construction/catalogs/`: final website catalog artifacts. This folder contains the JSON files for D1, D2, and D3, crawl-ready URL CSVs, and the D2-to-global mappings.


## 2. Crawling and PageGraph Instrumentation

Takes a CSV of sites to crawl as input, and records the resulting artifacts for each site: Pagegraphs, Har files, and screenshots.

### Run command

```bash
python crawling/controller.py site_catalogs_construction/catalogs/urls_to_crawl/D1.csv
```

The controller resolves the current country from the machine IP and writes crawl artifacts under:

```text
data/snapshots/<country>/<bucket>/<site>/<attempt_id>/
```

An SQLite database is created as `running_<csv_name>.db` to track crawl progress and resume interrupted jobs.

### Docker image for crawls

The repository includes scripts to build a Docker image for running the crawler on  different machine. The image is defined in `Dockerfile` and bundles the dependencies required by PageGraph-based crawling, including Node.js, Python, Xvfb, and browser libraries.

The script `Docker_build.sh` builds and saves the image under `global-privacy-ssh:latest`.

```bash
./Docker_build.sh
```

The image can then be transferred and loaded on another machine:

```bash
docker load -i global-privacy-ssh-latest.tar
```

Once loaded, the environment can be started as an execution node:

```bash
docker run --rm -it -p 2222:22 --name global-privacy-crawler global-privacy-ssh:latest
```


## 3. Preprocessing of crawl artifacts

Data Preprocessing consists of converting crawl artifacts into a SQL database for downstream analysis. It organized inthree steps. 


### 3.1 Processing Graph files

This step takes the raw PageGraph outputs stored in data/ and converts them into structured JSON records.

What it does:
- scans site directories under an input directory,
- extracts compressed `.gz` GraphML payloads,
- runs the PageGraph query CLI over each GraphML file,
- collects cookies, scripts, requests, JS calls, HTML elements, and related metadata,

How to run it:

```bash
python pre_processing/process_graphml/main.py \
  --base-dir data/snapshots/DE/country_coded
```

It produces a combined JSON file for a country/site_catalog, saved under  `data/files_to_analyze/`.


### 3.2 Creating and populating the database

This step creates the relational database and uploads the ∂ata from the JSON files.

What it does:
- Reads MySql credentials from the .env file,
- creates the target MySQL database if it does not exist,
- builds the required tables for crawl sessions, cookies, storage, scripts, requests, JS calls, HTML elements, HAR requests, and tracking classification,

How to run it:

```bash
python pre_processing/process_database/create_db.py
```

Then populate the database with the processed crawl JSONL file:

```bash
python pre_processing/process_database/insert_file_into_db.py data/files_to_analyze/XX.jsonl.zst
```


### 3.3 Feature extraction

This final step derives privacy and security features from the raw crawl data stored in the database. It adds new columns and labels for identifiers, tracking behavior, and fingerprinting signals.

What it does:
- reads the processed crawl records from the database,
- extracts cookie and storage values,
- identifies user identifiers and tracking-related requests,
- analyzes JS calls for fingerprinting techniques,
- labels sessions and requests based on whether they match tracking or fingerprinting patterns.

How to run it:

```bash
python pre_processing/process_database/pre_process.py
```


## 4. Analysis

The analysis folder contains the descriptive and statistical comparison scripts we use to study how privacy and security signals vary across location and site catalogs.




## Data and Environment Notes

- The repository expects a `.env` configuration file. Use the same structure as in `.env_template`.
- The raw crawl data is large and not included in this repository.


