import os
import json
import html
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from neo4j import GraphDatabase
from openai import AzureOpenAI
from dotenv import load_dotenv
from lib.pipeline.rank import rank_candidates
from lib.pipeline.cypher import build_cypher
from lib.pipeline.justify import attach_justifications
from lib.pipeline.normalize_query import normalize_query_params

_APP_DIR = Path(__file__).resolve().parent

# Load `.env` from app directory (Streamlit cwd is not always `poc_wbce`).
load_dotenv(_APP_DIR / ".env")
load_dotenv()


def _env_strip(name: str, default: str | None = None) -> str | None:
    raw = os.getenv(name, default)
    if raw is None:
        return None
    s = str(raw).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"":
        s = s[1:-1].strip()
    return s or None


# Setup Neo4j Driver
URI = _env_strip("NEO4J_URI", "bolt://localhost:7687") or "bolt://localhost:7687"
USER = _env_strip("NEO4J_USER", "neo4j") or "neo4j"
PASSWORD = _env_strip("NEO4J_PASSWORD", "password") or "password"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# Setup Azure OpenAI client (supports Tribli names + standard Azure env names)
_api_key = _env_strip("OAI_KEY_LLM") or _env_strip("AZURE_OPENAI_API_KEY")
_endpoint = _env_strip("OAI_BASE_LLM") or _env_strip("AZURE_OPENAI_ENDPOINT")
_api_version = (
    _env_strip("OAI_VERSION")
    or _env_strip("OPENAI_API_VERSION")
    or "2024-12-01-preview"
)

llm_client: AzureOpenAI | None
if _api_key and _endpoint:
    llm_client = AzureOpenAI(
        api_key=_api_key,
        api_version=_api_version,
        azure_endpoint=_endpoint,
    )
else:
    llm_client = None

LLM_MODEL = _env_strip("LLM_MODEL_NAME", "gpt-5-mini") or "gpt-5-mini"


def decompose_prompt(user_query):
    if llm_client is None:
        raise RuntimeError(
            "Azure OpenAI credentials are missing. Set OAI_KEY_LLM and OAI_BASE_LLM "
            "(or AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT) in "
            f"{_APP_DIR / '.env'} — see .env.example."
        )
    system_prompt = """
    You are the "Prompt-Decomposition Layer" for a talent search engine.
    Your job is to read a natural language search query and extract key parameters.

    You must output a raw JSON object (and nothing else, no markdown formatting) with these exact keys:
    - "craft": (string) The role they are looking for (e.g., "director", "actor"). null if not specified.
    - "location": (string) Legacy location string (often a city like "hyderabad"). null if not specified.
    - "location_city": (string) The city, if explicitly specified. null if not specified.
    - "location_state": (string) The state/region, if explicitly specified (e.g., "uttar pradesh"). null if not specified.
    - "location_raw": (string) Copy of the location phrase only (not the full user query) if you can isolate it; otherwise null.
    - "banner": (string) A production banner they have worked with (e.g., "mythri"). null if not specified.
    - "keywords": (list of strings) Atomic descriptive words. Extract single words, not phrases (e.g., ["mass", "thriller"]). empty list if none.
      CRITICAL: Only include keywords that describe craft specializations, genres, or technical skills.
      DROP conversational filler, time references, budget references, and availability words.
    - "gender": (string) "male" or "female" if the query explicitly mentions a gender (e.g., "guy", "brother", "actress"). otherwise null.
    - "relationship_hint": (string) If the query uses relationship words like "brother" / "sister", output that single word; otherwise null.
    - "age_range": (string) One of "young", "mid", "senior", or null. Map "young", "emerging", "new", "junior" to "young" (typically age <= 30). Map "senior", "veteran", "experienced" to "mid" unless clearly over 50 then "senior". If no age signal, null.
    - "tier": (integer or null) Production scale for the role they need: 1 = mega-budget pan-India, 2 = big-budget Telugu theatrical, 3 = mid-budget, 4 = indie, 5 = micro-budget/shorts. null if no budget/scale signal (e.g. no mention of budget, scale, blockbuster, indie, short film).
    - "height_min_cm": (integer or null) If user specified a height, normalize to a lower bound in centimeters; otherwise null.
    - "height_max_cm": (integer or null) If user specified a height, normalize to an upper bound in centimeters; otherwise null.

    IMPORTANT: Do NOT repeat the input, do NOT add explanations, and do NOT produce any other text.
    """

    response = llm_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query},
        ],
    )

    raw_output = response.choices[0].message.content.strip()
    if raw_output.startswith("```json"):
        raw_output = raw_output[7:-3]
    return json.loads(raw_output)


def execute_cypher(query):
    with driver.session() as session:
        result = session.run(query)
        return [dict(record) for record in result]


# -----------------
# STREAMLIT UI
# -----------------
st.set_page_config(page_title="Talent Search", page_icon="🔍", layout="centered")

st.markdown(
    """
<style>
.profile-card {
    background-color: #ffffff;
    padding: 24px;
    border-radius: 8px;
    margin-bottom: 16px;
    border: 1px solid #e0e0e0;
    box-shadow: 0 2px 4px rgba(0,0,0,0.02);
}
.profile-header {
    margin-bottom: 12px;
}
.profile-name {
    font-size: 1.4em;
    font-weight: 600;
    color: #111111;
    margin: 0 0 8px 0;
}
.profile-meta {
    font-size: 0.9em;
    color: #666666;
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
}
.meta-item {
    display: flex;
    align-items: center;
}
.profile-bio {
    color: #333333;
    font-size: 1em;
    line-height: 1.6;
    margin-top: 12px;
}
.justification {
    color: #555555;
    font-size: 0.95em;
    line-height: 1.55;
    margin-top: 12px;
    padding-top: 8px;
    border-top: 1px solid #eeeeee;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("Talent Search Engine")

if llm_client is None:
    st.warning(
        "Azure OpenAI is not configured (`OAI_KEY_LLM` / `OAI_BASE_LLM` missing). "
        f"Add them to `{_APP_DIR / '.env'}` (see `.env.example`), then restart Streamlit."
    )

tab_search, tab_structured, tab_schema = st.tabs(["Search", "Structured Query", "Schema Reference"])

if "ranked_results" not in st.session_state:
    st.session_state["ranked_results"] = None
if "query_params" not in st.session_state:
    st.session_state["query_params"] = None
if "llm_upgrade_pending" not in st.session_state:
    st.session_state["llm_upgrade_pending"] = False

with tab_search:
    user_query = st.text_input(
        "Search query:", placeholder="e.g. Find me a director in Hyderabad..."
    )

    if st.button("Search") and user_query:
        with st.spinner("Searching and scoring (fast mode)..."):
            try:
                params = decompose_prompt(user_query)
            except Exception as e:
                st.error(f"Error calling LLM: {e}")
                st.stop()

            params = normalize_query_params(user_query, params)
            if params.get("tier") is not None:
                try:
                    params["tier"] = int(params["tier"])
                except (TypeError, ValueError):
                    params["tier"] = None

            cypher_query = build_cypher(params)
            results = execute_cypher(cypher_query)
            ranked_results = rank_candidates(results, params, driver)
            # Fast first paint: deterministic justifications for top 10.
            ranked_results = attach_justifications(
                ranked_results,
                params,
                driver,
                limit=10,
                llm_client=None,
                model=None,
            )
            st.session_state["ranked_results"] = ranked_results
            st.session_state["query_params"] = params
            st.session_state["llm_upgrade_pending"] = bool(llm_client)

    ranked_results = st.session_state.get("ranked_results")
    if ranked_results:
        for r in ranked_results:
            name = r.get("Name", "Unknown Name")
            age = r.get("Age", "N/A")
            craft = r.get("Craft", "Craft Not Specified")
            region = r.get("Region", "Region Not Specified")
            bio = r.get("Bio", "")
            ccs = r.get("ccs_total", 0)
            fs = r.get("final_score", 0)

            just = r.get("justification")
            just_html = ""
            if just:
                just_html = f'<p class="justification">{html.escape(just)}</p>'

            card_html = f"""
            <div class="profile-card">
                <div class="profile-header">
                    <h3 class="profile-name">{name}</h3>
                    <div class="profile-meta">
                        <span class="meta-item">{craft.title()}</span> •
                        <span class="meta-item">{region.title()}</span> •
                        <span class="meta-item">{age} years old</span> •
                        <span class="meta-item">CCS {ccs:.4f}</span> •
                        <span class="meta-item">Score {fs:.4f}</span>
                    </div>
                </div>
                <div class="profile-bio">
                    {bio}
                </div>
                {just_html}
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)

        # Optional quality pass: refine only top 3 justifications with LLM.
        # This keeps first response fast and avoids waiting on 10 LLM calls.
        if st.session_state.get("llm_upgrade_pending") and llm_client is not None:
            with st.status("Refining top 3 explanations...", expanded=False):
                try:
                    refined = attach_justifications(
                        ranked_results,
                        st.session_state.get("query_params") or {},
                        driver,
                        limit=3,
                        llm_client=llm_client,
                        model=LLM_MODEL,
                        timeout_sec=6,
                        max_workers=3,
                    )
                    merged = list(ranked_results)
                    for idx in range(min(3, len(merged), len(refined))):
                        merged[idx]["justification"] = refined[idx].get("justification")
                    st.session_state["ranked_results"] = merged
                except Exception as e:
                    st.warning(f"LLM refinement skipped: {e}")
                finally:
                    st.session_state["llm_upgrade_pending"] = False
            st.rerun()
    elif st.session_state.get("ranked_results") == []:
        st.info("No professionals found matching your criteria.")

with tab_structured:
    qp = st.session_state.get("query_params")
    if qp is None:
        st.info("Run a search to see the structured query JSON.")
    else:
        st.caption(
            "This is the exact structured JSON used for retrieval + ranking. "
            "It includes the LLM decomposition plus deterministic normalization."
        )
        st.json(qp)

with tab_schema:
    schema_path = _APP_DIR / "layered_schema.html"
    if schema_path.exists():
        schema_html = schema_path.read_text(encoding="utf-8")
        components.html(schema_html, height=2800, scrolling=True)
    else:
        st.error(f"Schema file not found at `{schema_path}`. Please add `layered_schema.html` to the project root.")
