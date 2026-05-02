from .factors import phi, h, delta, v, gamma
from .constants import WEIGHTS_GAMMA
from datetime import datetime

def compute_ccs(candidate_data: dict, query_context: dict) -> dict:
    total_ccs = 0.0
    contributions = []
    current_year = datetime.now().year

    g_score = gamma(candidate_data.get("context", {}), query_context, WEIGHTS_GAMMA)
    query_tier = query_context.get("tier", 3)
    credits = candidate_data.get("credits", [])

    def sort_key(c):
        year = c.get("year", 2000)
        role = c.get("role", "")
        return year + h(role) * 10
    
    credits = sorted(credits, key=sort_key, reverse=True)[:20]

    for credit in credits:
        p_score = phi(credit.get("tier", 3), query_tier)
        h_score = h(credit.get("role", ""))
        d_score = delta(credit.get("year", current_year), current_year)
        v_score = v(credit.get("verifiers", [])) if credit.get("verifiers") else 0.5
        
        contribution = p_score * h_score * v_score * d_score * g_score
        total_ccs += contribution
        
        contributions.append({
            "project_id": credit.get("project_id", "unknown"),
            "project_title": credit.get("title", "Unknown"),
            "phi": p_score,
            "h": h_score,
            "v": v_score,
            "delta": d_score,
            "gamma": g_score,
            "contribution": contribution
        })

    return {
        "total": total_ccs,
        "contributions": contributions
    }
