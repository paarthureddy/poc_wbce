import math
from .constants import LAMBDA_TIER, ROLE_WEIGHTS, MU_RECENCY

def phi(candidate_tier: int, query_tier: int) -> float:
    if not candidate_tier or not query_tier:
        return 0.5
    return math.exp(-LAMBDA_TIER * abs(candidate_tier - query_tier))

def h(role_string: str) -> float:
    if not role_string:
        return 0.15
    role_norm = role_string.lower().strip()
    return ROLE_WEIGHTS.get(role_norm, 0.5)

def delta(project_year: int, current_year: int) -> float:
    if not project_year:
        return 0.5
    diff = current_year - project_year
    if diff < 0:
        diff = 0
    return math.exp(-MU_RECENCY * diff)

def alpha(density: float) -> float:
    return 1.0 / (1.0 + density)

def v(verifiers: list) -> float:
    product = 1.0
    for ver in verifiers:
        a = alpha(ver.get('density', 0.0))
        prior = ver.get('prior', 0.5)
        product *= (1.0 - a * prior)
    return 1.0 - product

def gamma(candidate_context: dict, query_context: dict, weights: dict) -> float:
    camp_score = 0.5
    if query_context.get("banner"):
        q_banner = query_context["banner"].lower()
        c_banners = [b.lower() for b in candidate_context.get("banners", [])]
        if q_banner in c_banners:
            camp_score = 1.0
        else:
            camp_score = 0.0

    lineage_score = 0.5
    kin_score = 0.5
    
    region_score = 0.5
    if query_context.get("location"):
        q_loc = query_context["location"].lower()
        c_locs = [l.lower() for l in candidate_context.get("locations", [])]
        if any(q_loc in l for l in c_locs):
            region_score = 1.0
        else:
            region_score = 0.25

    return (weights["camp"] * camp_score +
            weights["lineage"] * lineage_score +
            weights["kin"] * kin_score +
            weights["region"] * region_score)
