"""
VariantAnnotationService
=========================
Web-layer integration for the Variant Annotator module.

This service is READ-ONLY: it never runs the annotation pipeline.
Annotation must be executed externally via Apptainer using:
    annotator/run_annotation.sh <PGS_ID>

Responsibilities:
  - Detect whether annotation outputs exist for a given PGS_ID
  - Read and serve the annotation summary JSON
  - Paginate and filter the annotated variant TSV.GZ
  - Build ideogram data structures (chromosome-level counts)
  - Return structured dicts ready for JSON serialisation or template rendering

Expected file layout under cfg.ANNOTATIONS_DIR / pgs_id:
    {PGS_ID}_variants_annotated.tsv.gz       (required for full table)
    {PGS_ID}_variants_annotated.parquet      (preferred; faster reads)
    {PGS_ID}_annotation_summary.json         (required for summary/status)
    annotation.log                            (optional; shown in UI)

Environment variable:
    APP_ANNOTATIONS_DIR   default: /annotations
"""
from __future__ import annotations

import json
import logging
import math
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.clinical_genes import get_clinical_genes_service
from services.variant_ranking import score_dataframe as _rank_score_df

logger = logging.getLogger(__name__)

# Columns returned by the paginated API.
# v1.0 columns are listed first for backward compatibility;
# v1.1+ / v1.2 columns are appended and may be absent on older annotation files
# — the service gracefully skips missing columns via available_cols filtering.
_TABLE_COLUMNS = [
    # betamap columns
    "PRS_ID", "CHROM", "POS", "ID",
    "EFFECT_ALLELE", "OTHER_ALLELE", "BETA", "IS_FLIP",
    # v1.0 annotation columns
    "gene_name", "gene_id", "gene_type",
    "transcript_id", "feature_type", "region_class", "consequence",
    "is_coding", "is_regulatory", "is_intergenic",
    "n_overlapping_genes", "strand", "distance_nearest_gene",
    # v1.1 annotation columns
    "distance_to_splice_site",
    "consequence_priority",
    "all_overlapping_genes",
    "all_overlapping_transcripts",
    "all_region_classes",
    "regulatory_source",
    # v1.2 annotation columns (FASTA + dbNSFP5 backed; absent on older files)
    "codon_ref",
    "codon_alt",
    "aa_ref",
    "aa_alt",
    "aa_ref_3",
    "aa_alt_3",
    "splice_type",
    "cadd_phred",
    "revel_score",
    "sift_pred",
    "polyphen2_pred",
    "clinvar_clnsig",
    "is_missense",
    "is_synonymous",
    "is_lof",
    # v1.3 annotation columns (dbSNP population frequency; absent on older files)
    "rsid",
    "af_global",
    "af_max_population",
    "af_population_summary",
    "rarity_class",
]

_DEFAULT_PAGE_SIZE = 500
_MAX_PAGE_SIZE     = 5_000


# ── JSON sanitisation ─────────────────────────────────────────────────────────

def clean_for_json(obj: Any) -> Any:
    """
    Recursively convert any value to a JSON-safe Python native type.

    Handles:
      - float NaN / Inf                → None
      - numpy integers                  → int
      - numpy floats (NaN/Inf aware)    → float or None
      - numpy bool_                     → bool
      - numpy ndarray                   → list (recursed)
      - pandas NA / NaT / pd.isna()    → None
      - dict                            → dict  (keys/values recursed)
      - list / tuple                    → list  (items recursed)
      - str / int / bool / None         → unchanged

    This ensures json.dumps() never produces the invalid NaN / Infinity literals.
    """
    # ── None fast-path ────────────────────────────────────────────────────────
    if obj is None:
        return None

    # ── Python float ─────────────────────────────────────────────────────────
    if type(obj) is float:
        return None if (math.isnan(obj) or math.isinf(obj)) else obj

    # ── Python bool / int / str: pass through unchanged ──────────────────────
    if type(obj) is bool or type(obj) is int or type(obj) is str:
        return obj

    # ── numpy scalars ─────────────────────────────────────────────────────────
    try:
        import numpy as np

        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            v = float(obj)
            return None if (math.isnan(v) or math.isinf(v)) else v
        if isinstance(obj, np.ndarray):
            return [clean_for_json(v) for v in obj.tolist()]
    except ImportError:
        pass

    # ── pandas NA / NaT ───────────────────────────────────────────────────────
    try:
        import pandas as pd
        # pd.isna() returns True for float NaN, pd.NA, pd.NaT, None
        # but raises TypeError for non-scalar containers; guard carefully.
        try:
            if pd.isna(obj):
                return None
        except (TypeError, ValueError):
            pass
    except ImportError:
        pass

    # ── Containers ────────────────────────────────────────────────────────────
    if isinstance(obj, dict):
        return {str(k): clean_for_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [clean_for_json(v) for v in obj]

    # ── Fallback: convert to str only for truly exotic types ──────────────────
    return obj


def _clean_rows(records: list) -> list:
    """Apply clean_for_json to every row dict in a list-of-dicts."""
    return [clean_for_json(row) for row in records]


# ── Service ───────────────────────────────────────────────────────────────────

class VariantAnnotationService:
    """
    Read-only service for variant annotation results.

    Instantiated once in app.py and injected into route handlers.
    Gracefully handles missing annotation outputs (status='not_annotated').
    All public methods return plain Python structures safe for json.dumps().
    """

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.annotations_dir: Path = cfg.ANNOTATIONS_DIR

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_status(self, pgs_id: str) -> Dict[str, Any]:
        """
        Return annotation status for pgs_id.

        Possible statuses:
            'annotated'     → summary JSON + TSV present
            'partial'       → TSV present but no summary (interrupted run)
            'running'       → annotation.log modified within last 30 min (heuristic)
            'not_annotated' → no output files found
        """
        out_dir      = self.annotations_dir / pgs_id
        summary_path = out_dir / f"{pgs_id}_annotation_summary.json"
        tsv_path     = out_dir / f"{pgs_id}_variants_annotated.tsv.gz"
        parquet_path = out_dir / f"{pgs_id}_variants_annotated.parquet"
        log_path     = out_dir / "annotation.log"

        has_summary = summary_path.exists()
        has_tsv     = tsv_path.exists()
        has_parquet = parquet_path.exists()
        has_log     = log_path.exists()

        if has_summary and has_tsv:
            status = "annotated"
        elif has_tsv and not has_summary:
            status = "partial"
        elif has_log and self._log_is_recent(log_path):
            status = "running"
        else:
            status = "not_annotated"

        out: Dict[str, Any] = {
            "pgs_id":      pgs_id,
            "status":      status,
            "has_summary": has_summary,
            "has_tsv":     has_tsv,
            "has_parquet": has_parquet,
            "has_log":     has_log,
            "output_dir":  str(out_dir),
        }

        if has_summary:
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                stats = summary.get("stats", {})
                out["annotated_at"]    = summary.get("annotated_at", "")
                out["total_variants"]  = stats.get("total_variants", 0)
                out["gff3_reference"]  = summary.get("gff3_reference", "")
                out["schema_version"]  = summary.get("schema_version", "1.0")
                out["fasta_reference"] = summary.get("fasta_reference")
                out["regulatory_beds"] = summary.get("regulatory_beds", [])
                out["n_splice_site"]   = stats.get("n_splice_site", 0)
                out["n_splice_region"] = stats.get("n_splice_region", 0)
            except Exception as exc:
                logger.warning("Cannot read summary for %s: %s", pgs_id, exc)

        return out

    def get_summary(self, pgs_id: str) -> Dict[str, Any]:
        """
        Return the full annotation summary JSON.
        Returns {'error': ...} if not available.
        The summary file is already valid JSON (written by the pipeline), so
        no NaN cleaning is needed here.
        """
        path = self.annotations_dir / pgs_id / f"{pgs_id}_annotation_summary.json"
        if not path.exists():
            return {"error": f"No annotation summary found for {pgs_id}.", "pgs_id": pgs_id}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Cannot parse summary for %s: %s", pgs_id, exc)
            return {"error": str(exc), "pgs_id": pgs_id}

    def get_ranked_variants(
        self,
        pgs_id: Optional[str],
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        min_score: float = 0.0,
        clinical_only: bool = False,
    ) -> Dict[str, Any]:
        """
        Return variants sorted descending by ranking_score.

        Ranking formula (see variant_ranking.py):
            score = 0.25*rarity + 0.30*consequence + 0.20*cadd_norm
                  + 0.10*revel_norm + 0.10*clinical_bonus + 0.05*lof_bonus

        Query parameters:
            pgs_id        : required (single PGS)
            min_score     : filter variants with score >= min_score (default 0.0)
            clinical_only : if True, only return variants in clinical genes
        """
        if not pgs_id:
            return {"error": "pgs_id is required.", "pgs_id": None}

        parquet_path = self.annotations_dir / pgs_id / f"{pgs_id}_variants_annotated.parquet"
        tsv_path     = self.annotations_dir / pgs_id / f"{pgs_id}_variants_annotated.tsv.gz"

        if not parquet_path.exists() and not tsv_path.exists():
            return {"error": f"No annotated variants found for {pgs_id}.", "pgs_id": pgs_id}

        try:
            df = self._load_annotated(parquet_path, tsv_path)
        except Exception as exc:
            return {"error": str(exc), "pgs_id": pgs_id}

        cg_svc   = get_clinical_genes_service()
        cg_syms  = cg_svc.get_all_symbols() if cg_svc.is_available() else set()

        df = df.copy()
        df["ranking_score"] = _rank_score_df(df, cg_syms)

        if clinical_only and "gene_name" in df.columns:
            df = df[df["gene_name"].str.upper().isin(cg_syms)]

        if min_score > 0:
            df = df[df["ranking_score"] >= min_score]

        df = df.sort_values("ranking_score", ascending=False).reset_index(drop=True)

        page_size   = max(1, min(page_size, _MAX_PAGE_SIZE))
        page        = max(1, page)
        total_rows  = len(df)
        total_pages = max(1, (total_rows + page_size - 1) // page_size)
        start       = (page - 1) * page_size
        end         = start + page_size

        rank_col = ["ranking_score"]
        available_cols = rank_col + [c for c in _TABLE_COLUMNS if c in df.columns]
        page_df = df[available_cols].iloc[start:end]
        raw_rows = page_df.to_dict(orient="records")
        clean_rows = _clean_rows(raw_rows)

        return {
            "pgs_id":       pgs_id,
            "page":         page,
            "page_size":    page_size,
            "total_rows":   total_rows,
            "total_pages":  total_pages,
            "columns":      available_cols,
            "rows":         clean_rows,
            "filters_applied": {
                "min_score":    min_score,
                "clinical_only": clinical_only,
            },
            "ranking_formula": {
                "weights": {
                    "rarity":      0.25,
                    "consequence": 0.30,
                    "cadd":        0.20,
                    "revel":       0.10,
                    "clinical":    0.10,
                    "lof":         0.05,
                },
                "description": (
                    "score = 0.25*rarity + 0.30*consequence + 0.20*cadd_norm"
                    " + 0.10*revel_norm + 0.10*clinical_bonus + 0.05*lof_bonus"
                ),
            },
        }

    def get_variants(
        self,
        pgs_id: Optional[str],
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        chrom: Optional[str] = None,
        region_class: Optional[str] = None,
        gene_name: Optional[str] = None,
        only_coding: bool = False,
        clinical_confidence: Optional[str] = None,
        add_ranking: bool = False,
        _scan_all: bool = False,
    ) -> Dict[str, Any]:
        """
        Return a paginated slice of the annotated variant table.

        Reads from Parquet if available (faster), otherwise from TSV.GZ.
        All values are sanitised through clean_for_json() before returning —
        NaN → null, numpy scalars → Python natives.

        Returns:
            {
              "pgs_id": str,
              "page": int,
              "page_size": int,
              "total_rows": int,
              "total_pages": int,
              "columns": [...],
              "rows": [...],          # list of row dicts, NaN-free
              "filters_applied": {...},
            }
        """
        page_size = max(1, min(page_size, _MAX_PAGE_SIZE))
        page      = max(1, page)

        # ── Gene-mode: scan all annotation dirs for variants in gene_name ──────
        if _scan_all and gene_name:
            return self._get_variants_for_gene(
                gene_name=gene_name,
                page=page,
                page_size=page_size,
                chrom=chrom,
                only_coding=only_coding,
            )

        if not pgs_id:
            return {"error": "pgs_id is required when _scan_all is False.", "pgs_id": None}

        parquet_path = self.annotations_dir / pgs_id / f"{pgs_id}_variants_annotated.parquet"
        tsv_path     = self.annotations_dir / pgs_id / f"{pgs_id}_variants_annotated.tsv.gz"

        if not parquet_path.exists() and not tsv_path.exists():
            return {
                "error": f"No annotated variants found for {pgs_id}.",
                "pgs_id": pgs_id,
            }

        try:
            df = self._load_annotated(parquet_path, tsv_path)
        except Exception as exc:
            logger.error("Cannot load variants for %s: %s", pgs_id, exc)
            return {"error": str(exc), "pgs_id": pgs_id}

        # ── Apply filters ─────────────────────────────────────────────────────
        filters_applied: Dict[str, Any] = {}
        if chrom:
            df = df[df["CHROM"].astype(str) == str(chrom)]
            filters_applied["chrom"] = chrom
        if region_class:
            # Support "splice_region" as a filter that covers both
            # splice_site_variant and splice_region_variant consequences.
            if region_class == "splice_region":
                df = df[df["consequence"].isin(
                    ["splice_site_variant", "splice_region_variant"]
                )]
            else:
                df = df[df["region_class"] == region_class]
            filters_applied["region_class"] = region_class
        if gene_name:
            # Match against best-hit gene_name OR all_overlapping_genes
            mask_best = df["gene_name"].str.lower() == gene_name.lower()
            if "all_overlapping_genes" in df.columns:
                mask_all = df["all_overlapping_genes"].str.lower().str.contains(
                    gene_name.lower(), na=False, regex=False
                )
                df = df[mask_best | mask_all]
            else:
                df = df[mask_best]
            filters_applied["gene_name"] = gene_name
        if only_coding:
            df = df[df["is_coding"].astype(bool)]
            filters_applied["only_coding"] = True

        # ── Clinical confidence filter ─────────────────────────────────────────
        # Filters rows where the variant's gene has the given confidence level.
        # high   → GenCC Definitive / Strong evidence
        # medium → GenCC Moderate
        # low    → GenCC Limited / other
        if clinical_confidence:
            cg_svc_tmp = get_clinical_genes_service()
            if cg_svc_tmp.is_available() and "gene_name" in df.columns:
                conf_lower = clinical_confidence.lower()
                allowed_syms = {
                    sym for sym in cg_svc_tmp.get_all_symbols()
                    if str(cg_svc_tmp.get_confidence(sym) or "").lower() == conf_lower
                }
                df = df[df["gene_name"].str.upper().isin(allowed_syms)]
                filters_applied["clinical_confidence"] = clinical_confidence

        # ── Optional ranking score column ─────────────────────────────────────
        if add_ranking and len(df) > 0:
            cg_svc_r = get_clinical_genes_service()
            cg_syms_r = cg_svc_r.get_all_symbols() if cg_svc_r.is_available() else set()
            df = df.copy()
            df["ranking_score"] = _rank_score_df(df, cg_syms_r)

        total_rows  = len(df)
        total_pages = max(1, (total_rows + page_size - 1) // page_size)
        start       = (page - 1) * page_size
        end         = start + page_size

        extra_cols  = ["ranking_score"] if (add_ranking and "ranking_score" in df.columns) else []
        available_cols = extra_cols + [c for c in _TABLE_COLUMNS if c in df.columns]
        page_df = df[available_cols].iloc[start:end]

        # ── Sanitise before serialisation (NaN → null) ────────────────────────
        raw_rows = page_df.to_dict(orient="records")
        clean_rows = _clean_rows(raw_rows)

        # ── Clinical gene annotation (gene-level, added at result level) ─────────
        cg_svc = get_clinical_genes_service()
        clinical_gene_info = None
        if gene_name:
            cg_entry = cg_svc.get_gene_info(gene_name)
            if cg_entry:
                clinical_gene_info = {
                    "is_clinical_gene":   True,
                    "sources":            cg_svc.get_clinical_sources(gene_name),
                    "confidence":         cg_svc.get_confidence(gene_name),
                    "moi":                cg_entry.get("moi"),
                    "evidence":           cg_entry.get("evidence"),
                    "disease":            cg_entry.get("disease"),
                }
            else:
                clinical_gene_info = {"is_clinical_gene": False, "sources": []}

        # ── Clinical gene stats across all rows (pct in clinical genes) ─────────
        if clean_rows and cg_svc.is_available():
            cg_symbols = cg_svc.get_all_symbols()
            n_clinical = sum(
                1 for r in clean_rows
                if r.get("gene_name", "").upper() in cg_symbols
            )
        else:
            n_clinical = 0

        return {
            "pgs_id":              pgs_id,
            "page":                page,
            "page_size":           page_size,
            "total_rows":          total_rows,
            "total_pages":         total_pages,
            "columns":             available_cols,
            "rows":                clean_rows,
            "filters_applied":     filters_applied,
            "clinical_gene_info":  clinical_gene_info,
            "n_clinical_in_page":  n_clinical,
        }

    def get_ideogram_data(self, pgs_id: str) -> Dict[str, Any]:
        """
        Build chromosome-level data for ideogram visualisation.
        All numeric values are sanitised (NaN-free) before returning.

        Contract (stable for future ideogram implementation):
        {
          "pgs_id": str,
          "chromosomes": [
            {
              "chrom":                "1",
              "n_variants":           int,
              "n_coding":             int,
              "n_intergenic":         int,
              "n_regulatory":         int,
              "region_class_counts":  {"coding": n, ...},
              "positions_coding":     [int, ...],    # up to 2000 per chrom
              "positions_intronic":   [int, ...],
              "positions_intergenic": [int, ...],
            }
          ],
          "chrom_order": ["1","2",...,"22","X","Y","MT"],
          "note": str
        }
        """
        parquet_path = self.annotations_dir / pgs_id / f"{pgs_id}_variants_annotated.parquet"
        tsv_path     = self.annotations_dir / pgs_id / f"{pgs_id}_variants_annotated.tsv.gz"

        if not parquet_path.exists() and not tsv_path.exists():
            return {"error": f"No annotated variants for {pgs_id}.", "pgs_id": pgs_id}

        try:
            df = self._load_annotated(parquet_path, tsv_path)
        except Exception as exc:
            return {"error": str(exc), "pgs_id": pgs_id}

        chrom_order = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
        POS_CAP = 2_000

        chromosomes: List[dict] = []
        for chrom in chrom_order:
            sub = df[df["CHROM"].astype(str) == chrom]
            if sub.empty:
                continue

            # region_class may contain NaN for unclassified rows; drop them
            rc_series = sub["region_class"].dropna().astype(str)
            rc_counts = dict(Counter(rc_series.tolist()))

            # Boolean columns: fill NaN with False before converting
            is_coding    = sub["is_coding"].fillna(False).astype(bool)
            is_intergenic= sub["is_intergenic"].fillna(False).astype(bool)
            is_regulatory= sub["is_regulatory"].fillna(False).astype(bool)

            # Positions: drop NaN and convert to plain int
            def _safe_positions(mask) -> list:
                pos_vals = sub.loc[mask, "POS"].dropna()
                return [int(p) for p in pos_vals.tolist()[:POS_CAP]]

            chromosomes.append({
                "chrom":                chrom,
                "n_variants":           len(sub),
                "n_coding":             int(is_coding.sum()),
                "n_intergenic":         int(is_intergenic.sum()),
                "n_regulatory":         int(is_regulatory.sum()),
                "region_class_counts":  rc_counts,
                "positions_coding":     _safe_positions(is_coding),
                "positions_intronic":   _safe_positions(sub["region_class"] == "intronic"),
                "positions_intergenic": _safe_positions(is_intergenic),
            })

        return {
            "pgs_id":      pgs_id,
            "chromosomes": chromosomes,
            "chrom_order": chrom_order,
            "note":        "Positions capped at 2000 per class per chromosome for UI performance.",
        }

    def get_run_command(self, pgs_id: str) -> Dict[str, Any]:
        """
        Return the suggested Apptainer run command for this PGS_ID.
        Shown in the UI when annotation has not been run yet.
        """
        data_dir        = str(self.cfg.DATA_DIR)
        annotations_dir = str(self.annotations_dir)
        return {
            "pgs_id": pgs_id,
            "script": f"/path/to/annotator/run_annotation.sh {pgs_id}",
            "env_vars": {
                "DATA_DIR":         data_dir,
                "ANNOTATIONS_DIR":  annotations_dir,
                "GFF3_PATH":        "/path/to/gencode.annotation.gff3",
                "SIF_PATH":         "/path/to/variant_annotator.sif",
                # Optional — set these to enable extended annotation
                "FASTA_PATH":       "(optional) /path/to/hg38.fa",
                "REGULATORY_BED":   "(optional) /path/to/encode_cCRE.bed.gz /path/to/ensembl_reg.bed.gz",
            },
            "apptainer_def":  "apptainer/variant_annotator.def",
            "build_command": (
                "apptainer build "
                "/path/to/variant_annotator.sif "
                "apptainer/variant_annotator.def"
            ),
            "note": (
                "Annotation runs on HPC nodes via Apptainer. The web platform only reads results. "
                "Set FASTA_PATH for future coding-consequence annotation. "
                "Set REGULATORY_BED for ENCODE/Ensembl Regulatory element-level annotation."
            ),
        }

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _load_annotated(self, parquet_path: Path, tsv_path: Path):
        """Load annotated variants, preferring Parquet over TSV.GZ."""
        import pandas as pd

        if parquet_path.exists():
            try:
                import pyarrow.parquet as pq
                return pq.read_table(str(parquet_path)).to_pandas()
            except Exception as exc:
                logger.warning(
                    "Cannot read parquet %s: %s — falling back to TSV", parquet_path, exc
                )

        if tsv_path.exists():
            return pd.read_csv(
                tsv_path, sep="\t",
                dtype={"CHROM": str, "POS": "int64"},
                compression="infer",
                low_memory=False,
            )

        raise FileNotFoundError("No annotated variant files found.")

    def _get_variants_for_gene(
        self,
        gene_name: str,
        page: int = 1,
        page_size: int = _DEFAULT_PAGE_SIZE,
        chrom: Optional[str] = None,
        only_coding: bool = False,
    ) -> Dict[str, Any]:
        """
        Scan all annotation parquets/TSVs for variants in the given gene.
        Used by the gene browser API endpoint.
        """
        import pandas as pd

        frames = []
        gene_lower = gene_name.lower()

        if not self.annotations_dir.exists():
            return {"error": "Annotations directory not found.", "gene_name": gene_name}

        for subdir in sorted(self.annotations_dir.iterdir()):
            if not subdir.is_dir():
                continue
            parquet_path = subdir / f"{subdir.name}_variants_annotated.parquet"
            tsv_path     = subdir / f"{subdir.name}_variants_annotated.tsv.gz"

            df_chunk = None
            try:
                df_chunk = self._load_annotated(parquet_path, tsv_path)
            except Exception:
                continue

            if df_chunk is None or df_chunk.empty:
                continue

            mask = pd.Series([False] * len(df_chunk), index=df_chunk.index)
            if "gene_name" in df_chunk.columns:
                mask = mask | (df_chunk["gene_name"].str.lower() == gene_lower)
            if "all_overlapping_genes" in df_chunk.columns:
                mask = mask | (
                    df_chunk["all_overlapping_genes"]
                    .fillna("").str.lower()
                    .str.contains(gene_lower, regex=False)
                )

            filtered = df_chunk[mask]
            if not filtered.empty:
                frames.append(filtered)

        if not frames:
            return {"error": f"No annotated variants found for gene '{gene_name}'.", "gene_name": gene_name}

        try:
            df = pd.concat(frames, ignore_index=True)
        except Exception as exc:
            return {"error": str(exc), "gene_name": gene_name}

        if chrom:
            df = df[df["CHROM"].astype(str) == str(chrom)]
        if only_coding and "is_coding" in df.columns:
            df = df[df["is_coding"].astype(bool)]

        df = df.sort_values("POS") if "POS" in df.columns else df

        total_rows  = len(df)
        total_pages = max(1, (total_rows + page_size - 1) // page_size)
        start = (page - 1) * page_size
        end   = start + page_size

        available_cols = [c for c in _TABLE_COLUMNS if c in df.columns]
        page_df = df[available_cols].iloc[start:end]
        raw_rows = page_df.to_dict(orient="records")
        clean_rows = _clean_rows(raw_rows)

        cg_svc = get_clinical_genes_service()
        cg_entry = cg_svc.get_gene_info(gene_name)
        clinical_gene_info = (
            {
                "is_clinical_gene": True,
                "sources":          cg_svc.get_clinical_sources(gene_name),
                "confidence":       cg_svc.get_confidence(gene_name),
                "moi":              cg_entry.get("moi"),
                "evidence":         cg_entry.get("evidence"),
                "disease":          cg_entry.get("disease"),
            }
            if cg_entry
            else {"is_clinical_gene": False, "sources": []}
        )

        return {
            "gene_name":          gene_name,
            "page":               page,
            "page_size":          page_size,
            "total_rows":         total_rows,
            "total_pages":        total_pages,
            "columns":            available_cols,
            "rows":               clean_rows,
            "filters_applied":    {"gene_name": gene_name},
            "clinical_gene_info": clinical_gene_info,
        }

    @staticmethod
    def _log_is_recent(log_path: Path, max_age_min: int = 30) -> bool:
        """Return True if the log file was modified within max_age_min minutes."""
        import time
        try:
            age_s = time.time() - log_path.stat().st_mtime
            return age_s < max_age_min * 60
        except OSError:
            return False
