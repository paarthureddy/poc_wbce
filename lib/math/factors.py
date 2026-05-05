from __future__ import annotations

import math
import re
from typing import Optional

from .constants import (
    LAMBDA_TIER,
    ROLE_WEIGHTS,
    MU_RECENCY,
    CAMP_SCORE_AFFILIATED,
    CAMP_SCORE_DIRECT_CREDIT,
    CAMP_SCORE_QUERY_BANNER_NO_MATCH,
    CAMP_SCORE_NEUTRAL,
    LINEAGE_SCORE_MATCH,
    LINEAGE_SCORE_NEUTRAL,
)


def phi(candidate_tier: Optional[int], query_tier: Optional[int]) -> float:
    """Tier alignment. Missing query tier is neutral (no penalty)."""
    if query_tier is None:
        return 1.0
    ct = candidate_tier if candidate_tier is not None else 3
    return math.exp(-LAMBDA_TIER * abs(ct - query_tier))


def _role_tokens(role_string: str) -> list[str]:
    if not role_string or not str(role_string).strip():
        return []
    raw = str(role_string).lower()
    parts = re.split(r"[,/&]| and |/", raw)
    tokens = []
    for p in parts:
        p = p.strip()
        if p:
            tokens.append(p)
    if not tokens:
        tokens.append(raw.strip())
    return tokens


def h(role_string: Optional[str]) -> float:
    """Best matching known role weight from compound role strings; unknown is neutral."""
    if not role_string or not str(role_string).strip():
        return 0.5
    best: float | None = None
    for tok in _role_tokens(role_string):
        w = ROLE_WEIGHTS.get(tok.strip())
        if w is not None:
            best = w if best is None else max(best, w)
    return best if best is not None else 0.5


def delta(project_year: int, current_year: Optional[int] = None) -> float:
    if not project_year:
        return 0.5
    if current_year is None:
        from datetime import datetime

        current_year = datetime.now().year
    diff = current_year - project_year
    if diff < 0:
        diff = 0
    return math.exp(-MU_RECENCY * diff)


def alpha(density: float) -> float:
    return 1.0 / (1.0 + density)


def v(verifiers: Optional[list]) -> float:
    if not verifiers:
        return 0.5
    product = 1.0
    for ver in verifiers:
        a = alpha(ver.get("density", 0.0))
        prior = ver.get("prior", 0.5)
        product *= (1.0 - a * prior)
    return 1.0 - product


def _banner_matches_query(q_banner: str, banner_names: list[str]) -> bool:
    q = q_banner.strip().lower()
    if not q:
        return False
    for b in banner_names:
        if not b:
            continue
        bl = str(b).lower()
        if q in bl or bl in q:
            return True
    return False


def _lineage_from_mentors(mentor_names: list[str], query_tokens: set[str]) -> float:
    if not mentor_names or not query_tokens:
        return LINEAGE_SCORE_NEUTRAL
    mentor_words: set[str] = set()
    for mn in mentor_names:
        if not mn:
            continue
        mentor_words.update(re.findall(r"[a-z0-9]+", str(mn).lower()))
    for tok in query_tokens:
        if len(tok) < 3:
            continue
        if tok in mentor_words:
            return LINEAGE_SCORE_MATCH
    return LINEAGE_SCORE_NEUTRAL


def gamma(candidate_context: dict, query_context: dict, weights: dict) -> float:
    """
    Contextual fit: camp (banner), region, lineage (mentorship keywords vs TRAINED_UNDER).
    Kin stays neutral until KINSHIP data exists in the graph.
    """
    affiliated = candidate_context.get("affiliated_banners") or []
    credited = candidate_context.get("credited_banners") or []
    # Back-compat: single list treated as affiliated-only
    legacy = candidate_context.get("banners") or []
    if legacy and not affiliated:
        affiliated = legacy

    camp_score = CAMP_SCORE_NEUTRAL
    qb = query_context.get("banner")
    if qb:
        q_banner = str(qb).lower()
        if _banner_matches_query(q_banner, affiliated):
            camp_score = CAMP_SCORE_AFFILIATED
        elif _banner_matches_query(q_banner, credited):
            camp_score = CAMP_SCORE_DIRECT_CREDIT
        else:
            camp_score = CAMP_SCORE_QUERY_BANNER_NO_MATCH

    query_tokens = set(query_context.get("_query_tokens") or [])
    mentor_names = candidate_context.get("mentor_names") or []
    lineage_score = _lineage_from_mentors(mentor_names, query_tokens)

    kin_score = 0.5

    region_score = 0.5
    if query_context.get("location"):
        q_loc = str(query_context["location"]).lower()
        c_locs = [str(l).lower() for l in candidate_context.get("locations", []) if l]
        if any(q_loc in l for l in c_locs):
            region_score = 1.0
        else:
            region_score = 0.25

    return (
        weights["camp"] * camp_score
        + weights["lineage"] * lineage_score
        + weights["kin"] * kin_score
        + weights["region"] * region_score
    )
