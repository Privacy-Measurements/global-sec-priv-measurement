import itertools
import pandas as pd


# Build value sets for each group.
def build_value_sets_by_group(df, group_col="group", value_col="value"):
    return {
        group_name: set(group[value_col].tolist())
        for group_name, group in df.groupby(group_col)
    }


# Compute shared/union/Jaccard/overlap metrics for two sets.
def compute_set_similarity_metrics(set_a, set_b):
    inter = len(set_a & set_b)
    union = len(set_a | set_b)
    min_size = min(len(set_a), len(set_b))

    jaccard_pct = round((inter / union) * 100, 2) if union > 0 else 0.0
    overlap_coefficient_pct = round((inter / min_size) * 100, 2) if min_size > 0 else 0.0

    return {
        "shared_count": inter,
        "union_count": union,
        "jaccard_pct": jaccard_pct,
        "overlap_coefficient_pct": overlap_coefficient_pct,
    }


# Build pairwise similarity rows for all group pairs.
def compute_pairwise_similarity_rows(value_sets, group_names):
    rows = []
    for group_a, group_b in itertools.combinations(group_names, 2):
        metrics = compute_set_similarity_metrics(
            value_sets.get(group_a, set()),
            value_sets.get(group_b, set()),
        )
        rows.append({
            "group_a": group_a,
            "group_b": group_b,
            **metrics,
        })
    return rows


# Build full NxN similarity matrices across groups.
def compute_similarity_matrices(value_sets, group_names, index_name="group"):
    intersection_count = pd.DataFrame(index=group_names, columns=group_names, dtype=int)
    jaccard_pct = pd.DataFrame(index=group_names, columns=group_names, dtype=float)
    overlap_coefficient_pct = pd.DataFrame(index=group_names, columns=group_names, dtype=float)

    for group_a in group_names:
        for group_b in group_names:
            metrics = compute_set_similarity_metrics(
                value_sets.get(group_a, set()),
                value_sets.get(group_b, set()),
            )
            intersection_count.loc[group_a, group_b] = metrics["shared_count"]
            jaccard_pct.loc[group_a, group_b] = metrics["jaccard_pct"]
            overlap_coefficient_pct.loc[group_a, group_b] = metrics["overlap_coefficient_pct"]

    intersection_count.index.name = index_name
    jaccard_pct.index.name = index_name
    overlap_coefficient_pct.index.name = index_name

    return intersection_count, jaccard_pct, overlap_coefficient_pct
