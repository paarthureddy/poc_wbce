# Core Credibility Score (CCS)

This prototype implements **CCS** (Core Credibility Score): a multiplicative credibility model over each candidate’s top credits, combined with contextual fit (Γ) and a final blend with query keyword relevance.

Historically the project used the informal label **WBCE** in discussion and in one seed-graph comment; **patent-facing and code-facing naming should use CCS**.

## Factors

| Symbol | Name | Role |
|--------|------|------|
| Φ | `phi` | Project tier alignment vs query tier (neutral when query tier is absent) |
| h | `h` | Role weight from normalized credit role |
| δ | `delta` | Recency of credit |
| V | `v` | Peer verification from co-credits and collaboration density |
| Γ | `gamma` | Contextual fit: camp (banner), region, lineage (mentorship), kin (reserved) |

## Aggregation

Per credit: `contribution = phi * h * v * delta * gamma`.

- Credits are **summed** (not averaged).
- At most **20** highest-priority credits are scored (latency bound).

## Trust multiplier

`verification_level` on the user applies a documented multiplier to the summed CCS before blending with keyword relevance.

## Lineage and kinship

- **Lineage**: Uses `TRAINED_UNDER` in the graph when `trained_under` was present in seed data. If query keywords align with a mentor name, lineage contributes; otherwise neutral.
- **Kinship**: No `KINSHIP` edges in current mock data — kin stays **neutral (0.5)** until data exists.

## Final ranking

`final_score = CCS_WEIGHT * normalized_ccs + KEYWORD_WEIGHT * normalized_keyword_score`

- CCS is normalized with a small **epsilon** so candidates with no project credits are not always crushed by tiny CCS noise elsewhere in the result set.
- Summed CCS is multiplied by an **experience-years boost** (bounded) and then by **verification_level**, before blending with keyword relevance.

Tunable weights and epsilon live in `lib/math/constants.py`.

If Neo4j warns that `TRAINED_UNDER` or `Project.tier` is missing, reload the graph with the current `seed_neo4j.py` so lineage and tier alignment match the mock dataset.

## Component 5 Justifications

After ranking, the UI can attach a short **grounded** explanation per top result using [`lib/pipeline/justify.py`](lib/pipeline/justify.py). The generator receives only that candidate’s evidence JSON (query, structured filters, credits, banners, mentors, verification, CCS credit breakdown) and must not reference other candidates or aggregate CCS numbers in user-facing copy.
