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

JUSTIFICATION_MAX_CHARS = 1800
LLM_DEFAULT_TIMEOUT_SEC = 14

_BANNED_COMPARATIVES = re.compile(
    r"\b(better than|worse than|higher rank|lower rank|ranked above|ranked below|"
    r"compared to|other candidates|other profiles|"
    r"number one|#1|first place|second place)\b",
    re.IGNORECASE,
)

# Tokens commonly used to describe complexion in profile data
_DARK_TOKENS = {"dark", "dusky", "wheatish", "tan", "tanned", "brown", "deep"}
_FAIR_TOKENS = {"fair", "light", "pale"}
_TALL_THRESHOLD_CM = 175
_SHORT_THRESHOLD_CM = 165


def _strip_internal_qc(qc: dict) -> dict:
    return {k: v for k, v in (qc or {}).items() if not str(k).startswith("_")}


def _as_plain_craft(value: Any) -> str:
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


def _norm_list(xs):
    return [str(x).strip() for x in (xs or []) if x and str(x).strip()]


def _contains_any(haystack: str, needles: set[str]) -> bool:
    hl = (haystack or "").lower()
    return any(n in hl for n in needles)


def _compute_query_alignment(
    sq: dict,
    facts: dict,
    bio: str,
    banners_a: list,
    banners_c: list,
    locations_graph: list,
) -> list[dict]:
    """
    For every dimension the user explicitly asked for, return a verdict:
      matched   - profile data supports it
      contradicts - profile data conflicts with it
      silent    - profile has no info either way
    Each entry includes the evidence string we used. Strictly grounded.
    """
    alignment: list[dict] = []

    def add(dim, asked, verdict, evidence):
        alignment.append({
            "dimension": dim,
            "asked_for": asked,
            "verdict": verdict,
            "evidence": evidence,
        })

    # --- Craft ---
    if sq.get("craft"):
        cand_craft = (facts.get("primary_craft") or "").lower()
        if cand_craft and sq["craft"].lower() in cand_craft:
            add("craft", sq["craft"], "matched", f"primary_craft = {facts.get('primary_craft')}")
        elif cand_craft:
            add("craft", sq["craft"], "contradicts", f"primary_craft = {facts.get('primary_craft')}")
        else:
            add("craft", sq["craft"], "silent", "no primary_craft on profile")

    # --- Location ---
    asked_loc = sq.get("location_city") or sq.get("location") or sq.get("location_state")
    if asked_loc:
        cand_loc_parts = [
            facts.get("location_city"),
            facts.get("location_state"),
            facts.get("location_country"),
        ]
        cand_loc_join = " | ".join([str(x) for x in cand_loc_parts if x]).lower()
        graph_loc_join = " | ".join(locations_graph).lower()
        if asked_loc.lower() in cand_loc_join or asked_loc.lower() in graph_loc_join:
            ev = cand_loc_join or graph_loc_join
            add("location", asked_loc, "matched", ev)
        elif cand_loc_join or graph_loc_join:
            add("location", asked_loc, "contradicts", cand_loc_join or graph_loc_join)
        else:
            add("location", asked_loc, "silent", "no location on profile")

    # --- Language ---
    if sq.get("language"):
        langs = [l.lower() for l in _norm_list(facts.get("languages_spoken"))]
        asked = sq["language"].lower()
        if any(asked in l or l in asked for l in langs):
            add("language", sq["language"], "matched", f"languages_spoken = {facts.get('languages_spoken')}")
        elif langs:
            add("language", sq["language"], "contradicts", f"languages_spoken = {facts.get('languages_spoken')}")
        else:
            # Check regional background as soft signal
            rb = (facts.get("regional_background") or "").lower()
            if rb and asked in rb:
                add("language", sq["language"], "matched", f"regional_background = {facts.get('regional_background')}")
            else:
                add("language", sq["language"], "silent", "no languages_spoken on profile")

    # --- Banner ---
    if sq.get("banner"):
        bn = sq["banner"].lower()
        all_banners = [b for b in (banners_a + banners_c) if b]
        matched = [b for b in all_banners if bn in str(b).lower()]
        if matched:
            add("banner", sq["banner"], "matched", f"banners = {matched[:3]}")
        elif all_banners:
            add("banner", sq["banner"], "silent", f"affiliated/credited banners present but no overlap: {all_banners[:3]}")
        else:
            add("banner", sq["banner"], "silent", "no banner edges in graph")

    # --- Gender ---
    if sq.get("gender"):
        g = (facts.get("gender") or "").lower()
        if g and sq["gender"].lower() == g:
            add("gender", sq["gender"], "matched", f"gender = {facts.get('gender')}")
        elif g:
            add("gender", sq["gender"], "contradicts", f"gender = {facts.get('gender')}")
        else:
            add("gender", sq["gender"], "silent", "no gender on profile")

    # --- Age range ---
    if sq.get("age_range"):
        age = facts.get("age")
        ar = sq["age_range"]
        if isinstance(age, (int, float)):
            in_range = (
                (ar == "young" and age <= 30)
                or (ar == "mid" and 30 < age <= 50)
                or (ar == "senior" and age > 50)
            )
            if in_range:
                add("age_range", ar, "matched", f"age = {int(age)}")
            else:
                add("age_range", ar, "contradicts", f"age = {int(age)}")
        else:
            add("age_range", ar, "silent", "no age on profile")

    # --- Physical / appearance keywords from query ---
    kws = [str(k).lower() for k in (sq.get("keywords") or [])]
    appearance_tags = [a.lower() for a in _norm_list(facts.get("appearance_tags"))]
    build_val = (facts.get("build") or "").lower()
    bio_l = (bio or "").lower()
    height = facts.get("height_cm")

    if "tall" in kws:
        if isinstance(height, (int, float)) and height >= _TALL_THRESHOLD_CM:
            add("physical:tall", "tall", "matched", f"height_cm = {int(height)}")
        elif isinstance(height, (int, float)) and height <= _SHORT_THRESHOLD_CM:
            add("physical:tall", "tall", "contradicts", f"height_cm = {int(height)}")
        elif "tall" in appearance_tags or "tall" in bio_l:
            add("physical:tall", "tall", "matched", "appearance tag or bio mentions 'tall'")
        else:
            add("physical:tall", "tall", "silent", "no height_cm or 'tall' tag on profile")

    if "short" in kws:
        if isinstance(height, (int, float)) and height <= _SHORT_THRESHOLD_CM:
            add("physical:short", "short", "matched", f"height_cm = {int(height)}")
        elif isinstance(height, (int, float)):
            add("physical:short", "short", "contradicts", f"height_cm = {int(height)}")
        else:
            add("physical:short", "short", "silent", "no height_cm on profile")

    if "dark" in kws or "dusky" in kws:
        joined = " ".join(appearance_tags) + " " + bio_l
        if _contains_any(joined, _DARK_TOKENS):
            add("physical:complexion", "dark", "matched", f"appearance/bio mentions dark/dusky tone")
        elif _contains_any(joined, _FAIR_TOKENS):
            add("physical:complexion", "dark", "contradicts", f"appearance/bio describes fair complexion")
        else:
            add("physical:complexion", "dark", "silent", "no complexion tag in profile")

    if "fair" in kws:
        joined = " ".join(appearance_tags) + " " + bio_l
        if _contains_any(joined, _FAIR_TOKENS):
            add("physical:complexion", "fair", "matched", f"appearance/bio mentions fair complexion")
        elif _contains_any(joined, _DARK_TOKENS):
            add("physical:complexion", "fair", "contradicts", f"appearance/bio describes dark complexion")
        else:
            add("physical:complexion", "fair", "silent", "no complexion tag in profile")

    # Any remaining keyword: search across appearance_tags, build, bio, tags_self
    body_blob = " ".join(
        appearance_tags
        + [build_val]
        + [t.lower() for t in _norm_list(facts.get("tags_self"))]
    ) + " " + bio_l
    physical_handled = {"tall", "short", "dark", "dusky", "fair"}
    for k in kws:
        if k in physical_handled or not k:
            continue
        if k in body_blob:
            add(f"keyword:{k}", k, "matched", "appears in profile tags or bio")
        else:
            add(f"keyword:{k}", k, "silent", "no direct mention in profile data")

    return alignment


def build_justification_evidence(
    ranked_row: dict,
    profile_facts: dict,
    candidate_data: dict,
    query_context: dict,
) -> dict:
    """Structured facts + CCS breakdown + query alignment for LLM or template."""
    qc = _strip_internal_qc(query_context or {})
    breakdown = list(ranked_row.get("ccs_breakdown") or [])
    breakdown_sorted = sorted(
        breakdown, key=lambda x: x.get("contribution", 0), reverse=True
    )
    top_contrib = float(breakdown_sorted[0].get("contribution") or 0) if breakdown_sorted else 0.0

    ctx = candidate_data.get("context") or {}
    credits = list(candidate_data.get("credits") or [])
    credits_with_peer_signal = sum(1 for c in credits if (c.get("verifiers") or []))
    years_on_graph = sorted(
        {int(c["year"]) for c in credits if c.get("year") is not None},
        reverse=True,
    )
    latest_year = years_on_graph[0] if years_on_graph else None

    title_to_credit = {c.get("title"): c for c in credits}
    top_credits_narrative = []
    for row in breakdown_sorted[:5]:
        title = row.get("project_title")
        gc = title_to_credit.get(title) or {}
        contrib = float(row.get("contribution") or 0)
        strength = "high" if top_contrib > 0 and contrib > top_contrib * 0.5 else "moderate"
        top_credits_narrative.append({
            "title": title,
            "year": gc.get("year"),
            "role_on_credit": gc.get("role"),
            "project_type": gc.get("project_type"),
            "peer_verifiers_on_this_credit": len(gc.get("verifiers") or []),
            "narrative_strength": strength,
            "recency_support": "recent" if float(row.get("delta") or 0) > 0.85 else "older",
        })

    uq = qc.get("raw_query") or ""
    query_numeric_tokens = re.findall(r"\b\d+\b", uq)

    structured_query = {
        "craft": qc.get("craft"),
        "location": qc.get("location"),
        "location_city": qc.get("location_city"),
        "location_state": qc.get("location_state"),
        "banner": qc.get("banner"),
        "language": qc.get("language"),
        "gender": qc.get("gender"),
        "relationship_hint": qc.get("relationship_hint"),
        "tier": qc.get("tier"),
        "age_range": qc.get("age_range"),
        "keywords": qc.get("keywords") or [],
        "height_min_cm": qc.get("height_min_cm"),
        "height_max_cm": qc.get("height_max_cm"),
    }

    bio_text = profile_facts.get("bio") or ranked_row.get("Bio") or ""

    query_alignment = _compute_query_alignment(
        sq=structured_query,
        facts=profile_facts,
        bio=bio_text,
        banners_a=ctx.get("affiliated_banners") or [],
        banners_c=ctx.get("credited_banners") or [],
        locations_graph=ctx.get("locations") or [],
    )

    evidence: dict[str, Any] = {
        "user_query": uq,
        "query_numeric_tokens": query_numeric_tokens,
        "structured_query": structured_query,
        "query_alignment": query_alignment,
        "candidate": {
            "id": ranked_row.get("id"),
            "name": profile_facts.get("name") or ranked_row.get("Name"),
            "age": profile_facts.get("age") if profile_facts.get("age") is not None else ranked_row.get("Age"),
            "gender": profile_facts.get("gender"),
            "primary_craft": _as_plain_craft(
                profile_facts.get("primary_craft") or ranked_row.get("Craft")
            ),
            "region": ranked_row.get("Region"),
            "location_city": profile_facts.get("location_city"),
            "location_state": profile_facts.get("location_state"),
            "location_country": profile_facts.get("location_country"),
            "regional_background": profile_facts.get("regional_background"),
            "experience_years": candidate_data.get("experience_years") or profile_facts.get("experience_years"),
            "verification_level": candidate_data.get("verification_level") or profile_facts.get("verification_level"),
            "peer_endorsement_count": profile_facts.get("peer_endorsement_count", 0),
            "languages_spoken": _norm_list(profile_facts.get("languages_spoken")),
            "height_cm": profile_facts.get("height_cm"),
            "build": profile_facts.get("build"),
            "appearance_tags": _norm_list(profile_facts.get("appearance_tags")),
            "looking_for": _norm_list(profile_facts.get("looking_for")),
            "tags_self": _norm_list(profile_facts.get("tags_self")),
            "bio_excerpt": bio_text[:500],
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
    """Deterministic markdown fallback used when LLM is unavailable or rejected."""
    cand = evidence["candidate"]
    sq = evidence["structured_query"]
    alignment = evidence.get("query_alignment") or []
    craft = (cand.get("primary_craft") or sq.get("craft") or "their craft").strip()
    region = cand.get("region") or cand.get("location_city") or ""
    exp_y = cand.get("experience_years")
    ver = cand.get("verification_level") or "unknown"
    n_credits = evidence["credit_summary"]["total_graph_credits"]
    peer_c = evidence["credit_summary"]["credits_with_peer_co_credit_signal"]
    endorse = int(cand.get("peer_endorsement_count") or 0)
    mentors = evidence.get("mentors") or []
    top = evidence.get("ccs_credit_breakdown_top") or []

    out: list[str] = []

    # Query alignment section — most important
    if alignment:
        match_lines = []
        gap_lines = []
        conflict_lines = []
        for a in alignment:
            dim = a["dimension"]
            asked = a["asked_for"]
            ev = a["evidence"]
            label = dim.split(":", 1)[-1] if ":" in dim else dim
            if a["verdict"] == "matched":
                match_lines.append(f"**{label}** ({asked}) — {ev}")
            elif a["verdict"] == "contradicts":
                conflict_lines.append(f"**{label}** ({asked}) — {ev}")
            else:
                gap_lines.append(f"**{label}** ({asked}) — {ev}")

        out.append("**Query Match**")
        if match_lines:
            for ln in match_lines:
                out.append(f"- ✅ {ln}")
        if conflict_lines:
            for ln in conflict_lines:
                out.append(f"- ⚠️ {ln}")
        if gap_lines:
            for ln in gap_lines:
                out.append(f"- ❔ Not shown in profile data — {ln}")

    # Credits
    out.append("")
    out.append("**Credibility Signals**")
    if n_credits == 0:
        out.append(
            "- On-record film and series credits are sparse, so ranking leans on profile fields and keyword alignment."
        )
    else:
        ylatest = evidence["credit_summary"].get("latest_credit_year")
        out.append(
            f"- {n_credits} on-record project credits, with peer co-credit corroboration on {peer_c} of them."
        )
        if ylatest:
            out.append(f"- Most recent on-record credit year: {ylatest}.")
        if top:
            titles = [t.get("title") for t in top[:3] if t.get("title")]
            if titles:
                out.append("- Key credited work: " + ", ".join(titles) + ".")

    if mentors:
        out.append(f"- Trained under {mentors[0]}.")

    # Verification / experience
    out.append("")
    out.append("**Profile**")
    out.append(
        f"- Craft: {craft}"
        + (f" • Region: {region}" if region else "")
        + (f" • Experience: {exp_y} years" if exp_y is not None else "")
    )
    out.append(f"- Verification status: {ver}.")
    if endorse:
        out.append(f"- Peer endorsements on platform: {endorse}.")

    text = "\n".join(out).strip()
    if len(text) > JUSTIFICATION_MAX_CHARS:
        text = text[: JUSTIFICATION_MAX_CHARS - 3] + "..."
    return text


def validate_justification(text: str, evidence: dict) -> bool:
    """Light validation: block comparatives, block invented decimals/percentages."""
    if not text or not text.strip():
        return False
    if _BANNED_COMPARATIVES.search(text):
        return False
    if len(text) > JUSTIFICATION_MAX_CHARS:
        return False
    # Block obvious invented stats — decimals and percentages
    if re.search(r"\b\d+\.\d+\b", text):
        return False
    if re.search(r"\d+\s*%", text):
        return False
    return True


def _llm_generate(client: Any, model: str, evidence: dict, timeout_sec: float) -> str:
    system = """You are a talent evaluation assistant for a film-industry talent search product. Your job is to explain to a busy producer/director WHY this specific candidate is being shown for THEIR query.

OUTPUT FORMAT (Markdown, scannable):
- Start with a **Query Match** section that addresses EACH dimension in `query_alignment`:
  - For verdict "matched": say so plainly with the supporting evidence (e.g., "Speaks Telugu — listed in languages_spoken").
  - For verdict "contradicts": flag the mismatch honestly (e.g., "Located in Mumbai, not Hyderabad as requested").
  - For verdict "silent": say the profile does not record this attribute (e.g., "Complexion not recorded in profile data — cannot confirm 'dark' on record").
- Then a **Credibility Signals** section: credits count, peer corroboration, latest year, key titles.
- Then a **Profile** section: craft, region/city, experience years, verification, mentors if any.

HARD RULES:
- Only state facts that appear in the evidence JSON. NEVER invent heights, languages, credits, or attributes that aren't there.
- If something the user asked for is silent in the data, SAY THAT explicitly — do not paper over it.
- Do not use Greek letters, do not mention "CCS", "score", "ranking position", or compare to other candidates.
- Do not include decimal scores or percentages.
- Keep total output under 200 words. Use short bullets, bold labels, emojis sparingly (✅ ⚠️ ❔ are fine).
- Be honest. A producer wants to know what fits AND what doesn't — they will make the final call."""

    user = (
        "Write the evaluation. Address every item in `query_alignment`.\n\nEVIDENCE_JSON:\n"
        + json.dumps(evidence, ensure_ascii=False, indent=2, default=str)
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
    Also attaches `_profile_facts` and `_candidate_context` so the UI can render a full profile view.
    """
    if not ranked_candidates:
        return []

    out = list(ranked_candidates)
    head = out[:limit]
    tail = out[limit:]

    def build_one(row: dict) -> tuple[str, str, dict, dict]:
        uid = row["id"]
        with driver.session() as session:
            facts = fetch_justification_facts(session, uid)
            cand = fetch_candidate_context(session, uid)
        evidence = build_justification_evidence(row, facts, cand, query_context)
        text = template_justification(evidence)
        if llm_client and model:
            try:
                raw = _llm_generate(llm_client, model, evidence, timeout_sec)
                # Preserve markdown line breaks — only collapse intra-line whitespace.
                raw = "\n".join(re.sub(r"[ \t]+", " ", ln).strip() for ln in raw.splitlines())
                raw = re.sub(r"\n{3,}", "\n\n", raw).strip()
                if validate_justification(raw, evidence):
                    text = raw
            except Exception:
                pass
        if not validate_justification(text, evidence):
            text = template_justification(evidence)
        return uid, text, facts, cand

    id_to_text: dict[str, str] = {}
    id_to_facts: dict[str, dict] = {}
    id_to_ctx: dict[str, dict] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(build_one, row): row for row in head}
        for fut in concurrent.futures.as_completed(futures, timeout=timeout_sec * limit + 5):
            row = futures[fut]
            try:
                uid, text, facts, cand = fut.result(timeout=timeout_sec + 5)
                id_to_text[uid] = text
                id_to_facts[uid] = facts
                id_to_ctx[uid] = cand
            except Exception:
                uid = row["id"]
                with driver.session() as session:
                    facts = fetch_justification_facts(session, uid)
                    cand = fetch_candidate_context(session, uid)
                ev = build_justification_evidence(row, facts, cand, query_context)
                id_to_text[uid] = template_justification(ev)
                id_to_facts[uid] = facts
                id_to_ctx[uid] = cand

    merged: list[dict] = []
    for row in head:
        r = dict(row)
        uid = row["id"]
        r["justification"] = id_to_text.get(uid) or template_justification(
            build_justification_evidence(
                row, id_to_facts.get(uid, {}),
                id_to_ctx.get(uid, {"context": {}, "credits": [], "experience_years": None, "verification_level": None}),
                query_context,
            )
        )
        r["_profile_facts"] = id_to_facts.get(uid, {})
        r["_candidate_context"] = id_to_ctx.get(uid, {})
        merged.append(r)
    for row in tail:
        r = dict(row)
        r["justification"] = None
        merged.append(r)
    return merged
