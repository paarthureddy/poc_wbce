"""
Component 5: grounded, query-aware justification paragraphs after CCS ranking.
"""
from __future__ import annotations

import json
import re
import concurrent.futures
from typing import Any

from ..graph.fetch_candidate_context import fetch_candidate_context
from ..graph.fetch_justification_facts import fetch_justification_facts

JUSTIFICATION_MAX_CHARS = 1500
JUSTIFICATION_MIN_SENTENCES = 1
JUSTIFICATION_MAX_SENTENCES = 20
LLM_DEFAULT_TIMEOUT_SEC = 14

_BANNED_COMPARATIVES = re.compile(
    r"\b(better than|worse than|higher rank|lower rank|ranked above|ranked below|"
    r"compared to|versus|vs\.| vs |unlike|other candidates|other profiles|"
    r"number one|#1|first place|second place)\b",
    re.IGNORECASE,
)


def _strip_internal_qc(qc: dict) -> dict:
    return {k: v for k, v in (qc or {}).items() if not str(k).startswith("_")}


def _as_plain_craft(value: Any) -> str:
    """Neo4j / Cypher sometimes returns nested structures, avoid dumping dicts into prose."""
    m: dict | None = None
    if isinstance(value, dict):
        m = value
    elif hasattr(value, "items") and not isinstance(value, (str, bytes, bytearray)):
        try:
            m = dict(value)
        except Exception:
            m = {}
    if m is not None:
        name = m.get("craft") or m.get("name") or m.get("subcraft")
        return str(name).strip() if name else "their craft"
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "their craft"


def _collect_allowed_number_strings(evidence: dict) -> set[str]:
    """Digits that may appear in a grounded justification (conservative whitelist)."""
    allowed: set[str] = set()

    def add_int(v: Any) -> None:
        if v is None:
            return
        if isinstance(v, bool):
            return
        if isinstance(v, int):
            allowed.add(str(v))
        elif isinstance(v, float) and v == int(v):
            allowed.add(str(int(v)))

    cand = evidence.get("candidate") or {}
    add_int(cand.get("age"))
    add_int(cand.get("experience_years"))
    add_int(cand.get("peer_endorsement_count"))

    cs = evidence.get("credit_summary") or {}
    add_int(cs.get("total_graph_credits"))
    add_int(cs.get("credits_with_peer_co_credit_signal"))
    add_int(cs.get("latest_credit_year"))
    for y in cs.get("years_present") or []:
        add_int(y)

    sq = evidence.get("structured_query") or {}
    add_int(sq.get("tier"))

    kr = evidence.get("keyword_relevance_score")
    if kr is not None:
        allowed.add(str(int(kr)))

    for t in evidence.get("ccs_credit_breakdown_top") or []:
        add_int(t.get("year"))
        add_int(t.get("peer_verifiers_on_this_credit"))

    for tok in evidence.get("query_numeric_tokens") or []:
        allowed.add(str(tok))

    for y in range(1990, 2040):
        allowed.add(str(y))
    return allowed


def build_justification_evidence(
    ranked_row: dict,
    profile_facts: dict,
    candidate_data: dict,
    query_context: dict,
) -> dict:
    """Structured facts + CCS breakdown slice for LLM or template (no other candidates)."""
    qc = _strip_internal_qc(query_context or {})
    breakdown = list(ranked_row.get("ccs_breakdown") or [])
    # Sort by contribution descending for narrative focus
    breakdown_sorted = sorted(
        breakdown, key=lambda x: x.get("contribution", 0), reverse=True
    )
    top_contrib = float(breakdown_sorted[0].get("contribution") or 0) if breakdown_sorted else 0.0

    ctx = candidate_data.get("context") or {}
    credits = list(candidate_data.get("credits") or [])
    credits_with_peer_signal = sum(
        1 for c in credits if (c.get("verifiers") or [])
    )
    years_on_graph = sorted(
        {int(c["year"]) for c in credits if c.get("year") is not None},
        reverse=True,
    )
    latest_year = years_on_graph[0] if years_on_graph else None

    # Enrich top credits with graph fields (role, type) by title match
    title_to_credit = {c.get("title"): c for c in credits}
    top_credits_narrative = []
    for row in breakdown_sorted[:5]:
        title = row.get("project_title")
        gc = title_to_credit.get(title) or {}
        contrib = float(row.get("contribution") or 0)
        strength = (
            "high"
            if top_contrib > 0 and contrib > top_contrib * 0.5
            else "moderate"
        )
        top_credits_narrative.append(
            {
                "title": title,
                "year": gc.get("year"),
                "role_on_credit": gc.get("role"),
                "project_type": gc.get("project_type"),
                "peer_verifiers_on_this_credit": len(gc.get("verifiers") or []),
                "narrative_strength": strength,
                "recency_support": "recent"
                if float(row.get("delta") or 0) > 0.85
                else "older",
            }
        )

    uq = qc.get("raw_query") or ""
    query_numeric_tokens = re.findall(r"\b\d+\b", uq)

    evidence: dict[str, Any] = {
        "user_query": uq,
        "query_numeric_tokens": query_numeric_tokens,
        "structured_query": {
            "craft": qc.get("craft"),
            "location": qc.get("location"),
            "banner": qc.get("banner"),
            "tier": qc.get("tier"),
            "age_range": qc.get("age_range"),
            "keywords": qc.get("keywords") or [],
        },
        "candidate": {
            "id": ranked_row.get("id"),
            "name": profile_facts.get("name") or ranked_row.get("Name"),
            "age": profile_facts.get("age") if profile_facts.get("age") is not None else ranked_row.get("Age"),
            "primary_craft": _as_plain_craft(
                profile_facts.get("primary_craft") or ranked_row.get("Craft")
            ),
            "region": ranked_row.get("Region"),
            "experience_years": candidate_data.get("experience_years"),
            "verification_level": candidate_data.get("verification_level"),
            "peer_endorsement_count": profile_facts.get("peer_endorsement_count", 0),
            "looking_for": profile_facts.get("looking_for") or [],
            "tags_self": profile_facts.get("tags_self") or [],
            "bio_excerpt": (profile_facts.get("bio") or ranked_row.get("Bio") or "")[:400],
        },
        "banners": {
            "affiliated": ctx.get("affiliated_banners") or [],
            "credited_via_projects": ctx.get("credited_banners") or [],
        },
        "mentors": ctx.get("mentor_names") or [],
        "credit_summary": {
            "total_graph_credits": len(credits),
            "credits_with_peer_co_credit_signal": credits_with_peer_signal,
            "latest_credit_year": latest_year,
            "years_present": years_on_graph[:8],
        },
        "ccs_credit_breakdown_top": top_credits_narrative,
        "keyword_relevance_score": ranked_row.get("keyword_score"),
        "notes": [],
    }

    if not credits:
        evidence["notes"].append("no_project_credits_in_graph")
    if not breakdown:
        evidence["notes"].append("no_ccs_credit_contributions")
    if qc.get("banner") and not (ctx.get("affiliated_banners") or ctx.get("credited_banners")):
        evidence["notes"].append("query_mentions_banner_but_candidate_has_no_banner_edges")

    return evidence


def _ensure_sentence_punctuation(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    if s[-1] not in ".!?":
        s += "."
    return s


def template_justification(evidence: dict) -> str:
    """Deterministic paragraph when LLM is unavailable or validation fails."""
    cand = evidence["candidate"]
    sq = evidence["structured_query"]
    name = cand.get("name") or "This professional"
    craft = (cand.get("primary_craft") or sq.get("craft") or "their craft").strip()
    region = cand.get("region") or ""
    exp_y = cand.get("experience_years")
    ver = cand.get("verification_level") or "unknown"
    n_credits = evidence["credit_summary"]["total_graph_credits"]
    peer_c = evidence["credit_summary"]["credits_with_peer_co_credit_signal"]
    endorse = int(cand.get("peer_endorsement_count") or 0)
    mentors = evidence.get("mentors") or []
    banners_a = evidence["banners"]["affiliated"]
    banners_c = evidence["banners"]["credited_via_projects"]
    kw = sq.get("keywords") or []
    kr = evidence.get("keyword_relevance_score")
    top = evidence.get("ccs_credit_breakdown_top") or []

    parts = []

    # Query alignment
    if sq.get("banner"):
        bn = str(sq["banner"]).lower()
        matched = [b for b in banners_a + banners_c if b and bn in str(b).lower()]
        if matched:
            parts.append(
                _ensure_sentence_punctuation(
                    f"{name} matches this search strongly through banner-related work tied to {', '.join(matched[:3])}"
                )
            )
        else:
            parts.append(
                _ensure_sentence_punctuation(
                    f"{name} appears in this banner-focused search primarily through other signals; banner overlap is not established in the graph data provided"
                )
            )
    elif kw:
        kw_list = ", ".join(kw[:5])
        if isinstance(kr, (int, float)) and kr > 0:
            parts.append(
                _ensure_sentence_punctuation(
                    f"{name} is surfaced here partly because the query keywords {kw_list} show overlap with profile and project-type signals alongside craft fit for {craft}"
                )
            )
        else:
            parts.append(
                _ensure_sentence_punctuation(
                    f"{name} is surfaced here mainly on craft and profile signals for {craft}; direct overlap for query keywords {kw_list} is limited in the on-record profile/project fields shown"
                )
            )
    else:
        parts.append(
            _ensure_sentence_punctuation(
                f"{name} is surfaced here based on craft and experience fit for {craft}"
                + (f" in {region}" if region else "")
            )
        )

    # Credits / verification
    if n_credits == 0:
        parts.append(
            _ensure_sentence_punctuation(
                "On-record film and series credits are sparse in the graph, so ranking leans more on profile fields and keyword alignment than on a deep credit stack"
            )
        )
    else:
        ylatest = evidence["credit_summary"].get("latest_credit_year")
        parts.append(
            _ensure_sentence_punctuation(
                f"The ranking draws on {n_credits} on-record project credits"
                f" with peer co-credit corroboration on {peer_c} of them"
            )
        )
        if ylatest:
            parts.append(
                _ensure_sentence_punctuation(
                    f"The most recent on-record credit year is {ylatest}"
                )
            )

    if top:
        titles = [t.get("title") for t in top[:3] if t.get("title")]
        if titles:
            parts.append(
                _ensure_sentence_punctuation(
                    "Key credited work called out in the credibility breakdown includes "
                    + ", ".join(titles)
                )
            )

    # Mentors / trust
    if mentors:
        tail = f" Verification status is {ver}." if ver else ""
        parts.append(
            _ensure_sentence_punctuation(
                f"Mentorship lineage includes training under {mentors[0]}{tail}"
            )
        )
    else:
        tail = (
            f" Experience on profile is {exp_y} years."
            if exp_y is not None
            else ""
        )
        parts.append(
            _ensure_sentence_punctuation(f"Verification status is {ver}{tail}")
        )

    if endorse:
        parts.append(
            _ensure_sentence_punctuation(
                f"Peers have left {endorse} on-platform endorsements tied to this profile"
            )
        )

    # Format as bullet points
    md_parts = [f"- {p}" for p in parts if p.strip()]
    text = "\n".join(md_parts)
    if len(text) > JUSTIFICATION_MAX_CHARS:
        text = text[: JUSTIFICATION_MAX_CHARS - 3] + "..."
    return text


def validate_justification(text: str, evidence: dict) -> bool:
    if not text or not text.strip():
        return False
    if _BANNED_COMPARATIVES.search(text):
        return False
    if len(text) > JUSTIFICATION_MAX_CHARS:
        return False
    # Sentence limits removed to allow markdown bullet points.
    allowed = _collect_allowed_number_strings(evidence)
    for num in re.findall(r"\b\d+\b", text):
        if num in allowed:
            continue
        if len(num) == 4 and num.startswith(("19", "20")):
            if int(num) in range(1990, 2040):
                continue
        return False
    return True


def _llm_generate(
    client: Any,
    model: str,
    evidence: dict,
    timeout_sec: float,
) -> str:
    system = """You are a talent evaluation assistant for a film-industry talent search product.
Write a highly structured, meaningful summary of why this candidate matches the search query. 
Rules:
- Format the output using Markdown. Use bolding, bullet points, and brief sections (e.g., **Key Alignment**, **Credit Highlights**, **Verification**).
- Keep it concise, punchy, and highly scannable for a busy recruiter, producer, or director.
- Do not use Greek letters, do not say CCS, do not quote numeric factor scores (no decimals like 0.85).
- Do not compare to other candidates or mention ranking position.
- Only state facts that appear in the JSON evidence. If something is missing, say it is not shown in the data instead of inventing it.
- Reflect the user's query: emphasize what they asked for (craft, location, banner, genre keywords, tier) when relevant."""

    user = (
        "Write the justification paragraph.\n\nEVIDENCE_JSON:\n"
        + json.dumps(evidence, ensure_ascii=False, indent=2)
    )

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        timeout=timeout_sec,
    )
    return (resp.choices[0].message.content or "").strip()


def attach_justifications(
    ranked_candidates: list[dict],
    query_context: dict,
    driver: Any,
    limit: int = 10,
    llm_client: Any | None = None,
    model: str | None = None,
    timeout_sec: float = LLM_DEFAULT_TIMEOUT_SEC,
    max_workers: int = 5,
) -> list[dict]:
    """
    Mutates copies of ranked rows in the top `limit` slice by setting `justification`.
    """
    if not ranked_candidates:
        return []

    out = list(ranked_candidates)
    head = out[:limit]
    tail = out[limit:]

    def build_one(row: dict) -> tuple[str, str, dict]:
        uid = row["id"]
        with driver.session() as session:
            facts = fetch_justification_facts(session, uid)
            cand = fetch_candidate_context(session, uid)
        evidence = build_justification_evidence(row, facts, cand, query_context)
        text = template_justification(evidence)
        if llm_client and model:
            try:
                raw = _llm_generate(llm_client, model, evidence, timeout_sec)
                raw = re.sub(r"\s+", " ", raw).strip()
                if validate_justification(raw, evidence):
                    text = raw
            except Exception:
                pass
        if not validate_justification(text, evidence):
            text = template_justification(evidence)
        return uid, text, evidence

    id_to_text: dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(build_one, row): row for row in head}
        for fut in concurrent.futures.as_completed(futures, timeout=timeout_sec * limit + 5):
            row = futures[fut]
            try:
                uid, text, _ev = fut.result(timeout=timeout_sec + 5)
                id_to_text[uid] = text
            except Exception:
                uid = row["id"]
                with driver.session() as session:
                    facts = fetch_justification_facts(session, uid)
                    cand = fetch_candidate_context(session, uid)
                ev = build_justification_evidence(row, facts, cand, query_context)
                id_to_text[uid] = template_justification(ev)

    merged: list[dict] = []
    for row in head:
        r = dict(row)
        r["justification"] = id_to_text.get(row["id"], template_justification(
            build_justification_evidence(
                row,
                {},
                {"context": {}, "credits": [], "experience_years": None, "verification_level": None},
                query_context,
            )
        ))
        merged.append(r)
    for row in tail:
        r = dict(row)
        r["justification"] = None
        merged.append(r)
    return merged
