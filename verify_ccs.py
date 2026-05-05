#!/usr/bin/env python3
"""
Deterministic CCS verification: five evaluator-style queries + veteran vs newcomer pair.

Requires Neo4j with data loaded (e.g. python seed_neo4j.py). Run from poc_wbce:

    python verify_ccs.py
"""
from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from neo4j import GraphDatabase

from lib.pipeline.cypher import build_cypher
from lib.pipeline.rank import rank_candidates

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


def _execute(driver, cypher: str, **params) -> list[dict[str, Any]]:
    with driver.session() as session:
        return [dict(r) for r in session.run(cypher, **params)]


def _print_top(label: str, baseline: list[dict], ranked: list[dict], top: int = 5) -> None:
    print(f"\n=== {label} ===")
    print("Baseline order (first %d):" % top)
    for i, r in enumerate(baseline[:top], 1):
        print(
            f"  {i}. {r.get('id')} | {r.get('Name')} | kw={r.get('keyword_score', 0)}"
        )
    print("After CCS blend (first %d):" % top)
    for i, r in enumerate(ranked[:top], 1):
        print(
            f"  {i}. {r.get('id')} | {r.get('Name')} | "
            f"ccs={r.get('ccs_total', 0):.4f} | final={r.get('final_score', 0):.4f} | "
            f"kw={r.get('keyword_score', 0)}"
        )


def _expect_in_top(ranked: list[dict], ids: list[str], within: int) -> bool:
    top_ids = [r.get("id") for r in ranked[:within]]
    return any(e in top_ids for e in ids)


def main() -> None:
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    results_summary: list[tuple[str, str]] = []

    eval_cases: list[tuple[str, dict, list[str], int]] = [
        (
            "experienced director in Hyderabad who works on mass entertainers",
            {
                "craft": "director",
                "location": "hyderabad",
                "keywords": ["mass", "entertainer", "commercial"],
                "tier": 2,
                "age_range": "mid",
                "raw_query": "experienced director in Hyderabad who works on mass entertainers",
            },
            ["user_g_007", "user_g_039"],
            3,
        ),
        (
            "young writer with thriller experience",
            {
                "craft": "writer",
                "age_range": "young",
                "keywords": ["thriller", "crime"],
                "tier": 4,
                "raw_query": "young writer with thriller experience",
            },
            ["user_g_004", "user_g_040"],
            3,
        ),
        (
            "director who has worked with Mythri Movie Makers",
            {
                "craft": "director",
                "banner": "mythri",
                "tier": 2,
                "raw_query": "director who has worked with Mythri Movie Makers",
            },
            ["user_g_001"],
            3,
        ),
        (
            "casting director for Telugu OTT",
            {
                "craft": "casting",
                "keywords": ["telugu", "ott"],
                "tier": 3,
                "raw_query": "casting director for Telugu OTT",
            },
            ["user_g_002", "user_g_034"],
            5,
        ),
        (
            "fitness influencer with brand experience",
            {
                "craft": "content",
                "keywords": ["fitness", "brand", "influencer"],
                "tier": 3,
                "raw_query": "fitness influencer with brand experience",
            },
            ["user_h_005"],
            5,
        ),
    ]

    try:
        for raw_q, params, expect_ids, within in eval_cases:
            p = dict(params)
            p["raw_query"] = raw_q
            if p.get("tier") is not None:
                p["tier"] = int(p["tier"])
            cy = build_cypher(p)
            baseline = _execute(driver, cy)
            ranked = rank_candidates(baseline, p, driver)
            _print_top(raw_q[:60] + "...", baseline, ranked)
            order_changed = [r.get("id") for r in baseline[:5]] != [
                r.get("id") for r in ranked[:5]
            ]
            ok = _expect_in_top(ranked, expect_ids, within)
            results_summary.append(
                (
                    raw_q[:50],
                    "PASS" if ok else "FAIL",
                )
            )
            print(
                f"  Check expect {expect_ids} in top {within}: {'PASS' if ok else 'FAIL'} | "
                f"order_changed={order_changed}"
            )

        # Obvious pair: user_g_007 vs user_g_023
        pair_label = "Obvious: user_g_007 vs user_g_023 (commercial director)"
        p2 = {
            "craft": "director",
            "keywords": ["commercial", "film"],
            "tier": 2,
            "age_range": "mid",
            "raw_query": "experienced director who works on commercial films",
        }
        p2["raw_query"] = p2["raw_query"]
        ids = ["user_g_007", "user_g_023"]
        qpair = """
        MATCH (u:User) WHERE u.id IN $ids
        OPTIONAL MATCH (u)-[:HAS_PRIMARY_CRAFT]->(craft:Craft)
        OPTIONAL MATCH (u)-[:LIVES_IN]->(loc:Location)
        OPTIONAL MATCH (u)-[:CREDITED_ON]->(p_keywords:Project)
        WITH u, craft, loc, collect(p_keywords.type) AS project_types
        WITH u, craft, loc, project_types,
             (CASE WHEN toLower(u.bio) CONTAINS toLower('commercial') THEN 1 ELSE 0 END) +
             (CASE WHEN toLower(u.bio) CONTAINS toLower('film') THEN 1 ELSE 0 END) +
             (CASE WHEN any(tag IN u.tags_self WHERE toLower(tag) CONTAINS toLower('commercial')) THEN 1 ELSE 0 END) +
             (CASE WHEN any(tag IN u.tags_self WHERE toLower(tag) CONTAINS toLower('film')) THEN 1 ELSE 0 END) +
             (CASE WHEN any(ptype IN project_types WHERE toLower(ptype) CONTAINS toLower('commercial')) THEN 1 ELSE 0 END) +
             (CASE WHEN any(ptype IN project_types WHERE toLower(ptype) CONTAINS toLower('film')) THEN 1 ELSE 0 END)
             AS keyword_score
        RETURN DISTINCT u.id AS id, u.name AS Name, u.age AS Age, u.bio AS Bio,
               collect(DISTINCT craft.name)[0] AS Craft, collect(DISTINCT loc.name)[0] AS Region, keyword_score
        """
        baseline_p = _execute(driver, qpair, ids=ids)
        ranked_p = rank_candidates(baseline_p, p2, driver)
        _print_top(pair_label, baseline_p, ranked_p, top=2)
        by_id = {r["id"]: r for r in ranked_p}
        s7 = by_id.get("user_g_007", {}).get("ccs_total", 0)
        s23 = by_id.get("user_g_023", {}).get("ccs_total", 0)
        ratio = (s7 / s23) if s23 else float("inf")
        print(f"  CCS user_g_007={s7:.6f} user_g_023={s23:.6f} ratio={ratio:.2f}x")
        pair_ok = s7 > s23 and ratio >= 5.0
        results_summary.append((pair_label[:50], "PASS" if pair_ok else "FAIL"))

        print("\n--- Summary ---")
        for name, status in results_summary:
            print(f"  [{status}] {name}")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
