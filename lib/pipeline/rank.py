import os
from neo4j import GraphDatabase
from ..graph.fetch_candidate_context import fetch_candidate_context
from ..math.ccs import compute_ccs

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
        
    ranked_candidates = []
    try:
        with driver.session() as session:
            for cand in candidates:
                cand_data = fetch_candidate_context(session, cand["id"])
                ccs_result = compute_ccs(cand_data, query_context)
                
                cand_copy = cand.copy()
                cand_copy["ccs_total"] = ccs_result["total"]
                cand_copy["ccs_breakdown"] = ccs_result["contributions"]
                ranked_candidates.append(cand_copy)
                
        ranked_candidates.sort(key=lambda x: x["ccs_total"], reverse=True)
    finally:
        if should_close:
            driver.close()
            
    return ranked_candidates
