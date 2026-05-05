from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


_INDIA_STATES = {
    "andhra pradesh",
    "assam",
    "delhi",
    "goa",
    "karnataka",
    "kerala",
    "madhya pradesh",
    "maharashtra",
    "tamil nadu",
    "telangana",
    "uttar pradesh",
    "west bengal",
}

# Lightweight aliases to improve recall when users ask for a state but data is in cities.
# Keep this intentionally small and only for common demo cases.
_LOCATION_ALIASES: dict[str, list[str]] = {
    "uttar pradesh": ["lucknow", "noida", "ghaziabad", "kanpur", "varanasi"],
}


@dataclass(frozen=True)
class HeightRangeCm:
    min_cm: int
    max_cm: int
    target_cm: int | None = None


def _norm_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _contains_any(text_l: str, words: set[str]) -> str | None:
    # Word-boundary-ish match to avoid partial hits.
    for w in sorted(words, key=len, reverse=True):
        if re.search(rf"(?<!\w){re.escape(w)}(?!\w)", text_l):
            return w
    return None


def parse_location_from_text(raw_query: str) -> dict[str, str | None]:
    q = (raw_query or "").strip()
    ql = q.lower()

    state = _contains_any(ql, _INDIA_STATES)
    if state:
        return {
            "location_raw": q,
            "location_state": state,
            "location_city": None,
        }

    # If no explicit state, we treat any provided location string as "city-ish".
    return {"location_raw": q, "location_state": None, "location_city": None}


def _round_int(x: float) -> int:
    return int(round(x))


def _height_range(target_cm: int, tolerance_cm: int = 5) -> HeightRangeCm:
    return HeightRangeCm(
        min_cm=max(1, target_cm - tolerance_cm),
        max_cm=target_cm + tolerance_cm,
        target_cm=target_cm,
    )


def parse_height_from_text(raw_query: str) -> HeightRangeCm | None:
    q = (raw_query or "").lower()

    # cm (e.g. "176 cm", "176cm", "180 centimeters")
    m = re.search(r"(?<!\d)(\d{2,3})\s*(cm|centimeter|centimeters)\b", q)
    if m:
        cm = int(m.group(1))
        if 80 <= cm <= 250:
            return _height_range(cm, tolerance_cm=3)

    # feet + inches: 5'10, 5' 10", 5 ft 10 in, 6 feet, 6ft
    #  - group 1: feet
    #  - group 2: inches (optional). Accepts both `5'10` and `5 ft 10 in`.
    m = re.search(
        r"(?<!\d)(\d)\s*(?:'|ft|feet)\s*(?:(\d{1,2})(?:\s*(?:\"|in|inch|inches))?)?",
        q,
    )
    if m:
        ft = int(m.group(1))
        inch = int(m.group(2)) if m.group(2) is not None else 0
        if 3 <= ft <= 8 and 0 <= inch <= 11:
            cm = _round_int(ft * 30.48 + inch * 2.54)
            tol = 5 if m.group(2) is None else 3
            return _height_range(cm, tolerance_cm=tol)

    # phrase-only like "six feet" isn't reliably parseable without NLP; leave to LLM.
    return None


def detect_relationship_hint(raw_query: str) -> str | None:
    q = (raw_query or "").lower()
    for w in ["brother", "sister", "son", "daughter", "wife", "husband"]:
        if re.search(rf"(?<!\w){re.escape(w)}(?!\w)", q):
            return w
    return None


def normalize_query_params(raw_query: str, params: dict) -> dict:
    """
    Returns a new params dict with additional normalized fields.

    We keep the original keys from LLM output intact for backward compatibility.
    """
    out = dict(params or {})
    out["raw_query"] = raw_query

    # Relationship hint (inspectable, but not used as keyword)
    rel = _norm_str(out.get("relationship_hint")) or detect_relationship_hint(raw_query)
    out["relationship_hint"] = rel

    # Location: allow LLM to provide richer fields, otherwise infer from raw text.
    llm_city = _norm_str(out.get("location_city"))
    llm_state = _norm_str(out.get("location_state"))
    legacy_loc = _norm_str(out.get("location"))

    inferred = parse_location_from_text(raw_query)
    out["location_raw"] = _norm_str(out.get("location_raw")) or inferred.get("location_raw")
    out["location_state"] = llm_state or inferred.get("location_state")
    out["location_city"] = llm_city or (legacy_loc if legacy_loc and not out.get("location_state") else None)

    # Alias expansions for state-based searches (used by Cypher builder).
    state_l = (out.get("location_state") or "").lower().strip()
    out["location_aliases"] = _LOCATION_ALIASES.get(state_l, [])

    # Height: prefer structured cm bounds if present, else parse from text.
    hr: HeightRangeCm | None = None
    try:
        hmin = out.get("height_min_cm")
        hmax = out.get("height_max_cm")
        if hmin is not None and hmax is not None:
            hr = HeightRangeCm(min_cm=int(hmin), max_cm=int(hmax), target_cm=None)
    except (TypeError, ValueError):
        hr = None

    if hr is None:
        hr = parse_height_from_text(raw_query)

    if hr is not None:
        out["height_min_cm"] = hr.min_cm
        out["height_max_cm"] = hr.max_cm
        out["height_target_cm"] = hr.target_cm
    else:
        out.setdefault("height_min_cm", None)
        out.setdefault("height_max_cm", None)
        out.setdefault("height_target_cm", None)

    return out

