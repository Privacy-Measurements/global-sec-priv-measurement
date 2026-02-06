# “Same Click, Different Risks”: Geography as a Hidden Factor in Web Privacy and Security

This repository contains the code and data-processing pipeline used to measure **client-side security and privacy behaviors of websites** using PageGraph instrumentation.


The repository includes the following modules:
- Construction of global, country-coded, and country-specific website catalogs.
- Crawling websites using a PageGraph-instrumented browser.
- Preprocessing crawl artifacts into structured databases.
- Large-scale analysis of tracking, fingerprinting, user identification.

---

## Repository Structure

```
analysis/        # Analysis scripts (Python)
crawling/        # PageGraph-based crawler (Node.js)
crux_urls/       # URL collection and bucket construction
pre_processing/  # Graph and database preprocessing
README.md
```

## 1. `crux_urls/` — Website Catalog Construction

This directory contains all artifacts related to URL sourcing and website catalog construction.


### Contents

- **`crawl_raw_urls/`**  
  Raw top-site lists collected from the Chrome UX Report (CrUX) snapshot dated **August 18**.

- **`suffixes/`**  
  Lists of country-code and geographic TLDs used to construct regional catalogs (e.g., `.de`, `.ae`, `.berlin`, `.dubai`).

- **`buckets/`**  
  Final website catalogs used in the study:
  - **D1**: Globally popular websites  
  - **D2**: Country-coded versions of global websites  
  - **D3**: Country-specific popular websites  

- **`urls_to_crawl/`**  
  URL lists used for:
  - global catalog crawls,
  - VPN vs. physical vantage-point ablation experiments.


### Building Website Catalogs

Place CrUX URLs in raw_crux_urls/ and run 

```bash
cd crux_urls
python build_buckets.py
```

This script produces finalized URL lists under `buckets/`.


---

## 2. `crawling/` — PageGraph-based Crawler

This directory contains the **crawling infrastructure** used to visit websites with a PageGraph-instrumented Brave browser.

### Requirements

- Node.js **v20+**
- npm **v10+**
- A local PageGraph-enabled Brave browser build

### Installation

```bash
cd crawling
npm install
```

### Running Crawls

```bash
npm run pagegraph-crawl-using-given-urls "<PATH_TO_URL_FILE>"
```

- `<PATH_TO_URL_FILE>` should point to a file under `crux_urls/`

The crawler:
- launches PageGraph-instrumented browser instances,
- records execution graphs (`.graphml.gz`)


---

## 3. `pre_processing/` — Crawl Artifact Processing

This module converts raw PageGraph outputs into structured formats suitable for analysis.

```
pre_processing/
├── process_graphml/    # Converts .graphml → JSON
└── process_database/   # Inserts processed data into SQL database
```

### Components

- **`process_graphml/`**
  - Extract `.graphml.gz` files.
  - Parses `.graphml` files.
  - Extracts scripts, requests, js_calls, cookies, and html_elements from `.graphml` files
  - Outputs JSON files

- **`process_database/`**
  - Reads processed JSON files
  - Inserts data into a relational SQL database
  - Creates tables for according the schema in pre_processing/process_database/create_db.py

   
---

## 4. `analysis/` — Security & Privacy Analysis

This directory contains **Python analysis scripts** that operate over the populated database.

```
analysis/
├── tracking/
├── fingerprinting/
├── user_identification/
```

Python **3.9** is required.

## System Requirements

- **System Architecture**: Only x86_64 systems are supported; ARM-based machines are not supported.
- **Operating System**: Ubuntu 20.04+ recommended
- **Node.js**: v20+
- **npm**: v10+
- **Python**: 3.9
- **Database**: MySQL or compatible SQL database

---


## Environment Configuration

The crawling and preprocessing pipeline requires environment configuration via a `.env` file.

### Step 1: Create `.env`

```bash
cp .env_template .env
```

### Step 2: Configure `.env`


#### Crawl Configuration

```env
#### Crawl Configuration ####

BROWSER_FOR_PRECRAWL_PATH="./resources/pagegraph_brave_build/Static/brave"
BROWSER_PATH="./resources/pagegraph_brave_build/Static/brave"

MAX_CORES=12
PROXY_PORT=8901

CRAWLING_DEPTH=5
SAVE_SCREENSHOTS=true

MEASUREMENT_DELAY=25
PAGEGRAPH_TIMEOUT=20
NAVIGATION_TIMEOUT=60
```

#### Database Configuration

```env
#### Database Configuration ####

DB_HOST=XXXX
DB_USER=XXXX
DB_PASSWORD=XXXX
DB_NAME=XXXX
```

---


## Typical Workflow

1. Build website catalogs (`crux_urls/`)
2. Crawl websites using PageGraph (`crawling/`)
3. Process execution graphs (`pre_processing/process_graphml`)
4. Insert data into SQL database (`pre_processing/process_database`)
5. Run analysis scripts (`analysis/`)

---
