import os
import re
from neo4j import GraphDatabase

from ..graph.fetch_candidate_context import fetch_candidate_context
from ..math.ccs import compute_ccs
from ..math.constants import (
    FINAL_SCORE_CCS_WEIGHT,
    FINAL_SCORE_KEYWORD_WEIGHT,
    CCS_NORMALIZATION_EPSILON,
)


def _enriched_query_context(query_context):
    """Attach token set for lineage / keyword overlap inside gamma."""
    qc = dict(query_context or {})
    tokens = set()
    raw = qc.get("raw_query") or ""
    for tok in re.findall(r"[a-z0-9]+", raw.lower()):
        if tok:
            tokens.add(tok)
    for k in qc.get("keywords") or []:
        for tok in re.findall(r"[a-z0-9]+", str(k).lower()):
            if tok:
                tokens.add(tok)
    qc["_query_tokens"] = sorted(tokens)
    return qc


def rank_candidates(candidates, query_context, driver=None):
    if not candidates:
        return []

    should_close = False
    if driver is None:
        URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        USER = os.getenv("NEO4J_USER", "neo4j")
        PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
        driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
        should_close = True

    qc = _enriched_query_context(query_context)
    ranked_candidates = []
    try:
        with driver.session() as session:
            for cand in candidates:
                cand_data = fetch_candidate_context(session, cand["id"])
                ccs_result = compute_ccs(cand_data, qc)

                cand_copy = cand.copy()
                cand_copy["ccs_total"] = ccs_result["total"]
                cand_copy["raw_ccs_total"] = ccs_result["raw_ccs_total"]
                cand_copy["verification_multiplier"] = ccs_result["verification_multiplier"]
                cand_copy["experience_boost"] = ccs_result.get("experience_boost", 1.0)
                cand_copy["ccs_breakdown"] = ccs_result["contributions"]
                ranked_candidates.append(cand_copy)

        eps = CCS_NORMALIZATION_EPSILON
        max_ccs = max((c["ccs_total"] or 0) for c in ranked_candidates) or 0.0
        max_kw = max((c.get("keyword_score") or 0) for c in ranked_candidates) or 0.0
        max_ccs_denom = max_ccs + eps
        max_kw_n = max_kw if max_kw > 0 else 1.0

        for c in ranked_candidates:
            nc = ((c.get("ccs_total") or 0) + eps) / max_ccs_denom
            nk = (c.get("keyword_score") or 0) / max_kw_n
            c["normalized_ccs"] = nc
            c["normalized_keyword_score"] = nk
            c["final_score"] = (
                FINAL_SCORE_CCS_WEIGHT * nc + FINAL_SCORE_KEYWORD_WEIGHT * nk
            )

        ranked_candidates.sort(key=lambda x: x["final_score"], reverse=True)
    finally:
        if should_close:
            driver.close()

    return ranked_candidates
