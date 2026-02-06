import os
import json
import argparse
from typing import List, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv

try:
    from PIL import Image
    import pytesseract
except ImportError:
    Image = None
    pytesseract = None

# Load .env
load_dotenv()
NUM_THREADS = int(os.getenv("NUM_THREADS_PREPROCESSING", 1))

# Directory for output file: ../../data/files_to_analyze relative to this script
OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../data/files_to_analyze")
)
os.makedirs(OUTPUT_DIR, exist_ok=True)


CLOUDFLARE_KEYWORDS = [
    # ---- ENGLISH ----
    "cloudflare",
    "checking your browser",
    "verifying you are human",
    "are you a robot",
    "checking if the site connection is secure",
    "performance & security by cloudflare",
    "attention required!",
    "ddos protection",
    "just a moment",

    # ---- FRENCH (fr) ----
    "vérification de votre navigateur",
    "vérification que vous êtes humain",
    "êtes-vous un robot",
    "vérification si la connexion est sécurisée",
    "protection ddos",
    "attention requise",
    "sécurité par cloudflare",

    # ---- SPANISH (es) ----
    "comprobando su navegador",
    "verificando que eres humano",
    "¿eres un robot",
    "comprobando si la conexión es segura",
    "protección ddos",
    "atención requerida",

    # ---- GERMAN (de) ----
    "überprüfung ihres browsers",
    "überprüfen ob sie ein mensch sind",
    "sind sie ein roboter",
    "überprüfe ob die verbindung sicher ist",
    "ddos-schutz",
    "achtung erforderlich",

    # ---- ITALIAN (it) ----
    "controllo del browser",
    "verifica che sei umano",
    "sei un robot",
    "controllo della connessione sicura",
    "protezione ddos",
    "attenzione richiesta",

    # ---- DUTCH (nl) ----
    "controle van uw browser",
    "controleren of u een mens bent",
    "bent u een robot",
    "controleren of de verbinding veilig is",
    "ddos-beveiliging",
    "aandacht vereist",

    # ---- ARABIC (ar) ----
    "التحقق من المتصفح",
    "التحقق مما إذا كنت إنسانًا",
    "هل أنت روبوت",
    "التحقق من أمان الاتصال",
    "حماية ddos",
    "يتطلب الانتباه",

    # ---- HINDI / INDIAN (hi / en-IN) ----
    "ब्राउज़र की जाँच",
    "जाँच कर रहे हैं कि आप मानव हैं",
    "क्या आप रोबोट हैं",
    "कनेक्शन सुरक्षित है या नहीं जाँच",
    "ddos सुरक्षा",
    "ध्यान आवश्यक",
]


def output_path(country: str, category: str) -> str:
    base = f"CLOUDFLARE_DETECTION_{country}_{category}"
    return os.path.join(OUTPUT_DIR, base + ".json")


def extract_url_from_cookies(cookies_path: str) -> str:
    """
    Read the URL from the cookies JSON file.
    Raise an exception on error so the caller (worker) fails,
    and the parent can decide to skip that site.
    """
    try:
        with open(cookies_path, 'r', encoding='utf-8') as f:
            cookies_data = json.load(f)
        url = cookies_data.get("meta", {}).get("url", "unknown")
        return url or "unknown"
    except Exception as e:
        msg = f"Failed to read url from cookies file {cookies_path}: {e}"
        print(f"[WARN] {msg}")
        raise RuntimeError(msg)


def is_cloudflare_screenshot(image_path: str) -> bool:
    """
    OCR-based Cloudflare detection (simple).
    Raise an exception if OCR cannot be performed.
    """
    if Image is None or pytesseract is None:
        msg = "PIL/pytesseract not available, cannot run OCR"
        print(f"[WARN] {msg} for {image_path}")
        raise RuntimeError(msg)

    try:
        img = Image.open(image_path)
        text = pytesseract.image_to_string(img).lower()
    except Exception as e:
        msg = f"OCR failed for {image_path}: {e}"
        print(f"[WARN] {msg}")
        raise RuntimeError(msg)

    return any(keyword in text for keyword in CLOUDFLARE_KEYWORDS)


def iter_site_dirs(base_path: str) -> List[str]:
    """Return all directories inside base_path."""
    return sorted(
        os.path.join(base_path, d)
        for d in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, d))
    )


def find_valid_triplets(site_dir: str) -> List[Dict[str, str]]:
    """
    For each site dir, find:
      <basename>.graphml
      <basename>.png
      <basename>.cookies.json
    Returns list of dicts: {graphml, png, cookies}
    """
    valid: List[Dict[str, str]] = []

    try:
        items = os.listdir(site_dir)
    except Exception as e:
        print(f"[WARN] Cannot access directory {site_dir}: {e}")
        return valid

    files = [f for f in items if os.path.isfile(os.path.join(site_dir, f))]
    graphml_files = [f for f in files if f.endswith(".graphml")]

    for g in graphml_files:
        base = g[:-8]  # remove .graphml
        png = base + ".png"
        cookies = base + ".cookies.json"

        png_path = os.path.join(site_dir, png)
        cookies_path = os.path.join(site_dir, cookies)

        if os.path.exists(png_path) and os.path.exists(cookies_path):
            valid.append({
                "graphml": os.path.join(site_dir, g),
                "png": png_path,
                "cookies": cookies_path
            })

    return valid


def load_existing_results(country: str, category: str):
    path = output_path(country, category)

    if not os.path.exists(path):
        print("[INFO] No existing output file found, starting fresh.")
        return [], set()

    try:
        with open(path, "r", encoding="utf-8") as f:
            existing = json.load(f)

        if not isinstance(existing, list):
            print(f"[WARN] Existing output file {path} is not a list. Ignoring.")
            return [], set()

    except Exception as e:
        print(f"[WARN] Failed to load existing output file {path}: {e}")
        return [], set()

    treated_sites = {r.get("etld") for r in existing if r.get("etld") is not None}

    print(f"[INFO] Loaded {len(existing)} existing records from {path}")
    print(f"[INFO] Treated sites so far: {len(treated_sites)}")
    return existing, treated_sites


def process_single_site(args):
    """
    Worker function for a single site directory.

    If any exception happens (cookies parsing, OCR, etc.), it will be raised
    and handled by the parent process, which will skip this site.
    """
    site_dir, country, category = args
    etld = os.path.basename(site_dir)
    triplets = find_valid_triplets(site_dir)

    if not triplets:
        return []

    print(f"[INFO] {etld}: {len(triplets)} matching crawl files")

    site_results: List[Dict[str, object]] = []

    for t in triplets:
        url = extract_url_from_cookies(t["cookies"])
        cloudflare = is_cloudflare_screenshot(t["png"])

        site_results.append({
            "country": country,
            "category": category,
            "etld": etld,
            "url": url,
            "cloudflare": cloudflare,
        })

    return site_results


def save_results(results: List[Dict], country: str, category: str):
    """
    Save JSON:
      CLOUDFLARE_DETECTION_COUNTRY_CATEGORY.json
    Overwrites the file with the current full `results` list.
    """
    path = output_path(country, category)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Saved JSON → {path}")


def process(base_path: str,
            country: str,
            category: str,
            existing_results: List[Dict],
            treated_sites: set):

    results: List[Dict[str, object]] = list(existing_results)

    all_site_dirs = iter_site_dirs(base_path)
    print(f"[INFO] Found {len(all_site_dirs)} total site directories.")

    # filter out already-treated sites
    to_process = [
        d for d in all_site_dirs
        if os.path.basename(d) not in treated_sites
    ]
    print(f"[INFO] {len(to_process)} site directories to process after resume filtering.")
    print(f"[INFO] Using {NUM_THREADS} processes.")

    if not to_process:
        print("[INFO] Nothing left to do.")
        return results

    args_list = [
        (site_dir, country, category)
        for site_dir in to_process
    ]

    processed_sites = 0

    with ProcessPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_to_site = {
            executor.submit(process_single_site, args): args[0]
            for args in args_list
        }

        total_sites = len(future_to_site)

        for future in as_completed(future_to_site):
            site_dir = future_to_site[future]
            etld = os.path.basename(site_dir)
            processed_sites += 1

            pct = (processed_sites / total_sites) * 100
            print(f"[PROGRESS] {processed_sites} / {total_sites} new sites processed ({pct:.2f}%)")

            try:
                site_results = future.result()
                if site_results:
                    results.extend(site_results)
            except Exception as e:
                # Handle worker errors here: log and skip this site
                print(f"[ERROR] Exception while processing {site_dir} (etld={etld}): {e}")
                continue

            # periodic save every 100 newly processed sites
            if processed_sites % 100 == 0:
                print(f"[INFO] Intermediate save after {processed_sites} new sites.")
                save_results(results, country, category)

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--country", required=True)
    parser.add_argument("--category", required=True)
    args = parser.parse_args()

    base_path = os.path.abspath(args.base_path)

    existing_results, treated_sites = load_existing_results(args.country, args.category)

    results = process(
        base_path,
        args.country,
        args.category,
        existing_results,
        treated_sites,
    )

    print("saving final results")
    save_results(results, args.country, args.category)


if __name__ == "__main__":
    main()
