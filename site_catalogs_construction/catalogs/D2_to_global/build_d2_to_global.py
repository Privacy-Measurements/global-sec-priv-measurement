import csv, math
from collections import defaultdict
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent.parent
CACHE_CSV = REPO_ROOT / "D2_catalog_validation" / "results" / "d2_equivalent_best_similarity_cache_embedding_clip-vit-base-patch32_cpu.csv"
OUTPUT_DIR = THIS_DIR
THRESHOLD = 0.85


def main():
    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    by_country = defaultdict(list)

    with CACHE_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status", "").strip() != "compared":
                continue

            score_text = (row.get("best_score") or "").strip()
            try:
                score = float(score_text)
            except ValueError:
                continue

            if not math.isfinite(score) or score < THRESHOLD:
                continue

            best_equivalent = (row.get("best_equivalent") or "").strip()
            if not best_equivalent:
                continue

            country = (row.get("country") or "").strip().lower()
            d2_site = (row.get("d2_site") or "").strip()
            pool = (row.get("best_equivalent_pool") or "").strip()
            by_country[country].append((d2_site, best_equivalent, pool == "D1"))

    for country, rows in sorted(by_country.items()):
        out_path = output_dir / f"d2_to_global_{country.upper()}.csv"
        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["d2_site", "global_site", "in_d1"],
                lineterminator="\n",
            )
            writer.writeheader()
            for d2_site, global_site, in_d1 in rows:
                writer.writerow({"d2_site": d2_site, "global_site": global_site, "in_d1": in_d1})

    combined_path = output_dir / "D2_to_GLOBAL.csv"
    with combined_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["country", "D2_site", "equivalents"],
            lineterminator="\n",
        )
        writer.writeheader()
        for country, rows in sorted(by_country.items()):
            grouped = defaultdict(list)
            for d2_site, global_site, _in_d1 in rows:
                if global_site not in grouped[d2_site]:
                    grouped[d2_site].append(global_site)
            for d2_site, equivalents in grouped.items():
                writer.writerow({"country": country, "D2_site": d2_site, "equivalents": str(equivalents)})

    total = sum(len(v) for v in by_country.values())
    print(f"Done. {total} validated pairs across {len(by_country)} countries.")


if __name__ == "__main__":
    main()