"""
variant_ranking.py
==================
Interpretable variant ranking score for PRS variants.

Formula
-------
    score = (
        0.25 * rarity_score
      + 0.30 * consequence_score
      + 0.20 * cadd_norm
      + 0.10 * revel_norm
      + 0.10 * clinical_gene_bonus
      + 0.05 * lof_bonus
    )

All components are normalised to [0, 1].
Final score is in [0, 1] — higher = higher clinical priority.

Component definitions
---------------------
rarity_score      : ultra-rare=1.0, novel=0.9, rare=0.75, low_frequency=0.5,
                    common=0.0; unknown → 0.5 (conservative)
consequence_score : pLoF=1.0 → intergenic=0.01  (table below)
cadd_norm         : min(cadd_phred / 50, 1.0); 0 when absent
revel_norm        : revel_score clipped to [0,1]; 0 when absent
clinical_gene_bonus: 1.0 if gene is a curated clinical gene, else 0
lof_bonus         : 1.0 if is_lof flag is True, else 0
"""
from __future__ import annotations

from typing import Optional

# ── Weights ───────────────────────────────────────────────────────────────────

WEIGHTS: dict[str, float] = {
    "rarity":      0.25,
    "consequence": 0.30,
    "cadd":        0.20,
    "revel":       0.10,
    "clinical":    0.10,
    "lof":         0.05,
}

# ── Rarity component ──────────────────────────────────────────────────────────
# Maps rarity_class strings (from dbSNP annotation, v1.3) to scores.
# "novel" = no AF entry in dbSNP → assumed rare → high score.

RARITY_SCORES: dict[str, float] = {
    "ultra-rare":    1.00,
    "novel":         0.90,
    "rare":          0.75,
    "low_frequency": 0.50,
    "common":        0.00,
}
_RARITY_DEFAULT = 0.50  # conservative fallback for unknown strings

# ── Consequence component ─────────────────────────────────────────────────────

CONSEQUENCE_SCORES: dict[str, float] = {
    # pLoF
    "stop_gained":            1.00,
    "frameshift_variant":     1.00,
    "splice_donor_variant":   0.95,
    "splice_acceptor_variant":0.95,
    "start_lost":             0.90,
    "stop_lost":              0.85,
    # Missense / in-frame
    "missense_variant":       0.70,
    "inframe_insertion":      0.65,
    "inframe_deletion":       0.65,
    # Splice region
    "splice_site_variant":    0.60,
    "splice_region_variant":  0.55,
    # Synonymous / coding
    "synonymous_variant":     0.20,
    "coding_sequence_variant":0.15,
    # UTR
    "5_prime_UTR_variant":    0.10,
    "3_prime_UTR_variant":    0.10,
    # Non-coding
    "non_coding_exon_variant":0.08,
    "regulatory_region_variant":0.07,
    "intron_variant":         0.05,
    "upstream_gene_variant":  0.03,
    "intergenic_variant":     0.01,
}
_CSQ_DEFAULT = 0.01

# ── CADD normalisation ────────────────────────────────────────────────────────
CADD_NORM_MAX: float = 50.0  # CADD phred 50 → normalised 1.0


# ── Per-row scoring ───────────────────────────────────────────────────────────

def compute_score(
    rarity_class: Optional[str],
    consequence: Optional[str],
    cadd_phred: Optional[float],
    revel_score: Optional[float],
    is_clinical_gene: bool,
    is_lof: bool,
) -> dict:
    """
    Compute ranking score for a single variant row.

    Returns
    -------
    {
      "score":      float   # final [0,1] score
      "components": {       # individual [0,1] components before weighting
        "rarity", "consequence", "cadd", "revel", "clinical", "lof"
      }
    }
    """
    r   = RARITY_SCORES.get(str(rarity_class or "").lower(), _RARITY_DEFAULT)
    c   = CONSEQUENCE_SCORES.get(str(consequence or ""), _CSQ_DEFAULT)
    cad = min(float(cadd_phred) / CADD_NORM_MAX, 1.0) if cadd_phred is not None else 0.0
    rev = float(revel_score) if revel_score is not None else 0.0
    rev = max(0.0, min(rev, 1.0))
    cln = 1.0 if is_clinical_gene else 0.0
    lof = 1.0 if is_lof else 0.0

    score = (
        WEIGHTS["rarity"]      * r
        + WEIGHTS["consequence"] * c
        + WEIGHTS["cadd"]        * cad
        + WEIGHTS["revel"]       * rev
        + WEIGHTS["clinical"]    * cln
        + WEIGHTS["lof"]         * lof
    )

    return {
        "score": round(score, 4),
        "components": {
            "rarity":      round(r,   4),
            "consequence": round(c,   4),
            "cadd":        round(cad, 4),
            "revel":       round(rev, 4),
            "clinical":    round(cln, 4),
            "lof":         round(lof, 4),
        },
    }


# ── Vectorised DataFrame scoring ──────────────────────────────────────────────

def score_dataframe(df, clinical_gene_symbols: set) -> "pd.Series":
    """
    Compute ranking scores for an entire pandas DataFrame in one vectorised pass.

    Expected columns (all optional with graceful fallback):
        rarity_class, consequence, cadd_phred, revel_score, gene_name, is_lof

    Parameters
    ----------
    df                    : pandas.DataFrame of annotated variants
    clinical_gene_symbols : set of uppercased gene symbols from ClinicalGenesService

    Returns
    -------
    pandas.Series of float scores (same index as df), range [0, 1].
    """
    import pandas as pd

    n = len(df)

    # ── Rarity ────────────────────────────────────────────────────────────────
    if "rarity_class" in df.columns:
        rarity_vals = (
            df["rarity_class"]
            .fillna("novel")
            .str.lower()
            .map(lambda x: RARITY_SCORES.get(x, _RARITY_DEFAULT))
        )
    else:
        rarity_vals = pd.Series([_RARITY_DEFAULT] * n, index=df.index)

    # ── Consequence ───────────────────────────────────────────────────────────
    if "consequence" in df.columns:
        csq_vals = (
            df["consequence"]
            .fillna("intergenic_variant")
            .map(lambda x: CONSEQUENCE_SCORES.get(str(x), _CSQ_DEFAULT))
        )
    else:
        csq_vals = pd.Series([_CSQ_DEFAULT] * n, index=df.index)

    # ── CADD ──────────────────────────────────────────────────────────────────
    if "cadd_phred" in df.columns:
        cadd_vals = (
            pd.to_numeric(df["cadd_phred"], errors="coerce")
            .fillna(0.0)
            .clip(0, CADD_NORM_MAX) / CADD_NORM_MAX
        )
    else:
        cadd_vals = pd.Series([0.0] * n, index=df.index)

    # ── REVEL ─────────────────────────────────────────────────────────────────
    if "revel_score" in df.columns:
        revel_vals = (
            pd.to_numeric(df["revel_score"], errors="coerce")
            .fillna(0.0)
            .clip(0.0, 1.0)
        )
    else:
        revel_vals = pd.Series([0.0] * n, index=df.index)

    # ── Clinical gene bonus ───────────────────────────────────────────────────
    if "gene_name" in df.columns and clinical_gene_symbols:
        clin_vals = (
            df["gene_name"].str.upper().isin(clinical_gene_symbols).astype(float)
        )
    else:
        clin_vals = pd.Series([0.0] * n, index=df.index)

    # ── LoF bonus ─────────────────────────────────────────────────────────────
    if "is_lof" in df.columns:
        lof_vals = df["is_lof"].fillna(False).astype(bool).astype(float)
    else:
        lof_vals = pd.Series([0.0] * n, index=df.index)

    scores = (
        WEIGHTS["rarity"]      * rarity_vals
        + WEIGHTS["consequence"] * csq_vals
        + WEIGHTS["cadd"]        * cadd_vals
        + WEIGHTS["revel"]       * revel_vals
        + WEIGHTS["clinical"]    * clin_vals
        + WEIGHTS["lof"]         * lof_vals
    )
    return scores.round(4)


# ── Gene-level aggregation ────────────────────────────────────────────────────

def aggregate_gene_metrics(df, clinical_gene_symbols: set) -> list[dict]:
    """
    Aggregate per-gene metrics for the heatmap / summary table.

    Required columns: gene_name
    Optional: BETA, af_global, consequence, cadd_phred, revel_score,
              rarity_class, is_lof

    Returns list of dicts, one per gene, sorted by sum_abs_beta descending:
    {
      "gene_name":        str,
      "n_variants":       int,
      "sum_abs_beta":     float,
      "mean_af":          float | None,
      "mean_cadd":        float | None,
      "is_clinical_gene": bool,
      "consequence_counts": {csq: count, ...},
      "ranking_score_mean": float,
    }
    """
    import pandas as pd

    if "gene_name" not in df.columns:
        return []

    df = df.copy()
    df["_rank_score"] = score_dataframe(df, clinical_gene_symbols)

    results = []
    for gene, gdf in df.groupby("gene_name", sort=False):
        if not gene or str(gene).lower() in ("nan", "none", ""):
            continue

        n = len(gdf)
        sum_beta = float(gdf["BETA"].abs().sum()) if "BETA" in gdf.columns else 0.0
        mean_af  = (
            float(pd.to_numeric(gdf["af_global"], errors="coerce").mean())
            if "af_global" in gdf.columns else None
        )
        import math as _math
        if mean_af is not None and (_math.isnan(mean_af) or _math.isinf(mean_af)):
            mean_af = None

        mean_cadd = (
            float(pd.to_numeric(gdf["cadd_phred"], errors="coerce").mean())
            if "cadd_phred" in gdf.columns else None
        )
        if mean_cadd is not None and (_math.isnan(mean_cadd) or _math.isinf(mean_cadd)):
            mean_cadd = None

        csq_counts: dict = {}
        if "consequence" in gdf.columns:
            csq_counts = (
                gdf["consequence"].dropna().value_counts().to_dict()
            )

        mean_score = float(gdf["_rank_score"].mean())

        results.append({
            "gene_name":           str(gene),
            "n_variants":          n,
            "sum_abs_beta":        round(sum_beta, 6),
            "mean_af":             round(mean_af, 6) if mean_af is not None else None,
            "mean_cadd":           round(mean_cadd, 2) if mean_cadd is not None else None,
            "is_clinical_gene":    str(gene).upper() in clinical_gene_symbols,
            "consequence_counts":  {str(k): int(v) for k, v in csq_counts.items()},
            "ranking_score_mean":  round(mean_score, 4),
        })

    results.sort(key=lambda x: x["sum_abs_beta"], reverse=True)
    return results
