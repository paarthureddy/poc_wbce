from .factors import phi, h, delta, v, gamma
from .constants import (
    WEIGHTS_GAMMA,
    VERIFICATION_MULTIPLIER,
    EXPERIENCE_BOOST_MAX_YEARS,
    EXPERIENCE_BOOST_COEFF,
)
from datetime import datetime


def _verification_multiplier(level: str | None) -> float:
    if not level:
        return 1.0
    key = str(level).strip().lower()
    return VERIFICATION_MULTIPLIER.get(key, 1.0)


def compute_ccs(candidate_data: dict, query_context: dict) -> dict:
    total_ccs = 0.0
    contributions = []
    current_year = datetime.now().year

    g_score = gamma(candidate_data.get("context", {}), query_context, WEIGHTS_GAMMA)
    query_tier = query_context.get("tier")
    if query_tier is not None:
        query_tier = int(query_tier)
    credits = candidate_data.get("credits", [])

    def sort_key(c):
        year = c.get("year", 2000)
        role = c.get("role", "")
        return year + h(role) * 10

    credits = sorted(credits, key=sort_key, reverse=True)[:20]

    for credit in credits:
        p_score = phi(credit.get("tier") or 3, query_tier)
        h_score = h(credit.get("role", ""))
        d_score = delta(credit.get("year", current_year), current_year)
        v_score = v(credit.get("verifiers"))

        contribution = p_score * h_score * v_score * d_score * g_score
        total_ccs += contribution

        contributions.append(
            {
                "project_id": credit.get("project_id", "unknown"),
                "project_title": credit.get("title", "Unknown"),
                "phi": p_score,
                "h": h_score,
                "v": v_score,
                "delta": d_score,
                "gamma": g_score,
                "contribution": contribution,
            }
        )

    raw_total = total_ccs
    exp = int(candidate_data.get("experience_years") or 0)
    exp_boost = 1.0 + (
        min(max(exp, 0), EXPERIENCE_BOOST_MAX_YEARS)
        / float(EXPERIENCE_BOOST_MAX_YEARS)
        * EXPERIENCE_BOOST_COEFF
    )
    boosted = raw_total * exp_boost
    v_mult = _verification_multiplier(candidate_data.get("verification_level"))
    adjusted_total = boosted * v_mult

    return {
        "total": adjusted_total,
        "raw_ccs_total": raw_total,
        "experience_boost": exp_boost,
        "verification_multiplier": v_mult,
        "contributions": contributions,
    }
