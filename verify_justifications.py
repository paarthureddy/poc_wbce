#!/usr/bin/env python3
"""
Smoke-test Component 5 justifications (template + optional LLM).

Run from poc_wbce with Neo4j available:

    python verify_justifications.py
    set JUSTIFY_USE_LLM=1   # optional: use Azure OpenAI like app.py
"""
from __future__ import annotations

import os
import time

from dotenv import load_dotenv
from neo4j import GraphDatabase

from lib.pipeline.cypher import build_cypher
from lib.pipeline.justify import attach_justifications
from lib.pipeline.rank import rank_candidates

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


def _run_case(driver, label: str, raw_q: str, params: dict, use_llm: bool) -> float:
    p = dict(params)
    p["raw_query"] = raw_q
    if p.get("tier") is not None:
        p["tier"] = int(p["tier"])
    cy = build_cypher(p)
    with driver.session() as session:
        rows = [dict(r) for r in session.run(cy)]
    t0 = time.perf_counter()
    ranked = rank_candidates(rows, p, driver)
    llm_client = None
    model = None
    if use_llm:
        from openai import OpenAI

        llm_client = OpenAI(
            api_key=os.getenv("GEMINI_API_KEY"),
            base_url=os.getenv("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai/"),
        )
        model = os.getenv("LLM_MODEL_NAME", "gemini-2.5-flash")
    ranked = attach_justifications(
        ranked, p, driver, limit=10, llm_client=llm_client, model=model
    )
    elapsed = time.perf_counter() - t0

    print(f"\n=== {label} ({elapsed:.2f}s) ===")
    for i, r in enumerate(ranked[:3], 1):
        j = (r.get("justification") or "").replace("\n", " ")
        print(f"  {i}. {r.get('id')} | {r.get('Name')}")
        print(f"     {j[:320]}{'...' if len(j) > 320 else ''}")

    bad = []
    for i, r in enumerate(ranked[:10], 1):
        if not r.get("justification"):
            bad.append(f"missing justification at rank {i}")
        t = r["justification"]
        if "better than" in t.lower() or "other candidates" in t.lower():
            bad.append(f"comparative language at rank {i}")
    if bad:
        print("  CHECKLIST FAIL:", "; ".join(bad))
    else:
        print("  CHECKLIST OK: top 10 have text; no obvious banned comparative phrases.")
    if elapsed > 8.0:
        print(f"  WARN: pipeline took {elapsed:.2f}s (target under 8s).")
    return elapsed


def main() -> None:
    use_llm = os.getenv("JUSTIFY_USE_LLM", "").lower() in ("1", "true", "yes")
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        cases = [
            (
                "Commercial director",
                "experienced director who works on commercial films",
                {
                    "craft": "director",
                    "keywords": ["commercial", "film"],
                    "tier": 2,
                    "age_range": "mid",
                },
            ),
            (
                "Mythri director",
                "director who has worked with Mythri Movie Makers",
                {"craft": "director", "banner": "mythri", "tier": 2},
            ),
            (
                "Fitness influencer",
                "fitness influencer with brand experience",
                {
                    "craft": "content",
                    "keywords": ["fitness", "brand", "influencer"],
                    "tier": 3,
                },
            ),
        ]
        times = []
        for label, raw, p in cases:
            times.append(_run_case(driver, label, raw, p, use_llm))
        print(f"\nMax case time: {max(times):.2f}s")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
