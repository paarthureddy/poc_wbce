import os
import json
import streamlit as st
from neo4j import GraphDatabase
from openai import AzureOpenAI
from dotenv import load_dotenv
from lib.pipeline.rank import rank_candidates

# Load env variables
load_dotenv()

# Setup Neo4j Driver
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# Setup Azure OpenAI Client
llm_client = AzureOpenAI(
    api_key=os.getenv("OAI_KEY_LLM"),
    api_version=os.getenv("OAI_VERSION"),
    azure_endpoint=os.getenv("OAI_BASE_LLM")
)

LLM_MODEL = os.getenv("LLM_MODEL_NAME", "gpt-5-mini")

def decompose_prompt(user_query):
    system_prompt = """
    You are the "Prompt-Decomposition Layer" for a talent search engine.
    Your job is to read a natural language search query and extract key parameters.
    
    You must output a raw JSON object (and nothing else, no markdown formatting) with these exact keys:
    - "craft": (string) The role they are looking for (e.g., "director", "actor"). null if not specified.
    - "location": (string) The city they should live in (e.g., "hyderabad"). null if not specified.
    - "banner": (string) A production banner they have worked with (e.g., "mythri"). null if not specified.
    - "keywords": (list of strings) Atomic descriptive words. Extract single words, not phrases (e.g., ["mass", "action"]). empty list if none.
      CRITICAL: Only include keywords that describe craft specializations, genres, or technical skills. 
      DROP conversational filler, time references, budget references, and availability words.
    - "gender": (string) "male" or "female" if the query explicitly mentions a gender (e.g., "guy", "brother", "actress"). otherwise null.
    
    IMPORTANT: Do NOT repeat the input, do NOT add explanations, and do NOT produce any other text.
    """
    
    response = llm_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ]
    )
    
    raw_output = response.choices[0].message.content.strip()
    if raw_output.startswith("```json"):
        raw_output = raw_output[7:-3]
    return json.loads(raw_output)

def build_cypher(params):
    lines = ["MATCH (u:User)"]
    
    node_wheres = []
    if params.get("gender"):
        node_wheres.append(f"toLower(u.gender) = toLower('{params['gender']}')")
    if params.get("age_range"):
        if params["age_range"] == "young": node_wheres.append("u.age <= 30")
        elif params["age_range"] == "mid": node_wheres.append("u.age > 30 AND u.age <= 50")
        elif params["age_range"] == "senior": node_wheres.append("u.age > 50")
        
    if node_wheres:
        lines.append("WHERE " + " AND ".join(node_wheres))
    
    if params.get("craft"):
        craft = params["craft"]
        lines.append(f"MATCH (u)-[:HAS_PRIMARY_CRAFT]->(c:Craft) WHERE toLower(c.name) CONTAINS toLower('{craft}')")
        
    if params.get("location"):
        loc = params["location"]
        lines.append(f"MATCH (u)-[:LIVES_IN]->(l:Location) WHERE toLower(l.name) CONTAINS toLower('{loc}')")
        
    if params.get("banner"):
        banner = params["banner"]
        lines.append(f"MATCH (u)-[:CREDITED_ON]->(p_banner:Project)<-[:PRODUCED]-(b:Banner) WHERE toLower(b.name) CONTAINS toLower('{banner}')")
        
    STOP_WORDS = {
        "brother", "sister", "friend", "villain", "hero", "role", "character", 
        "soonish", "available", "budget", "experienced", "good", "best", 
        "need", "someone", "like", "prefer", "based", "looking", "want", "find"
    }

    physique_kws = params.get("physique", []) if isinstance(params.get("physique"), list) else ([params.get("physique")] if params.get("physique") else [])
    generic_kws = [k for k in (params.get("keywords") or []) if k.lower() not in STOP_WORDS]
    all_keywords = physique_kws + generic_kws

    if all_keywords:
        lines.append("WITH DISTINCT u")
        lines.append("OPTIONAL MATCH (u)-[:CREDITED_ON]->(p_keywords:Project)")
        lines.append("WITH u, collect(p_keywords.type) as project_types")
        
        score_cases = []
        for kw in all_keywords:
            kw_clean = kw.replace("'", "\\'")
            score_cases.append(f"(CASE WHEN toLower(u.bio) CONTAINS toLower('{kw_clean}') THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN any(tag IN u.tags_self WHERE toLower(tag) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN any(tag IN u.appearance_tags WHERE toLower(tag) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN toLower(u.build) CONTAINS toLower('{kw_clean}') THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN any(ptype IN project_types WHERE toLower(ptype) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)")
            
        lines.append("WITH u, (" + " + ".join(score_cases) + ") AS keyword_score")
        lines.append("OPTIONAL MATCH (u)-[:HAS_PRIMARY_CRAFT]->(craft:Craft)")
        lines.append("OPTIONAL MATCH (u)-[:LIVES_IN]->(loc:Location)")
        lines.append("RETURN DISTINCT u.id AS id, u.name AS Name, u.age AS Age, u.bio AS Bio, collect(DISTINCT craft.name)[0] AS Craft, collect(DISTINCT loc.name)[0] AS Region, keyword_score")
        lines.append("ORDER BY keyword_score DESC")
    else:
        lines.append("OPTIONAL MATCH (u)-[:HAS_PRIMARY_CRAFT]->(craft:Craft)")
        lines.append("OPTIONAL MATCH (u)-[:LIVES_IN]->(loc:Location)")
        lines.append("RETURN DISTINCT u.id AS id, u.name AS Name, u.age AS Age, u.bio AS Bio, collect(DISTINCT craft.name)[0] AS Craft, collect(DISTINCT loc.name)[0] AS Region, 0 AS keyword_score")
        
    return "\n".join(lines)

def execute_cypher(query):
    with driver.session() as session:
        result = session.run(query)
        return [dict(record) for record in result]

# -----------------
# STREAMLIT UI
# -----------------
st.set_page_config(page_title="Talent Search", page_icon="🔍", layout="centered")

st.markdown("""
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
</style>
""", unsafe_allow_html=True)

st.title("Talent Search Engine")

user_query = st.text_input("Search query:", placeholder="e.g. Find me a director in Hyderabad...")

if st.button("Search") and user_query:
    with st.spinner("Searching..."):
        try:
            params = decompose_prompt(user_query)
        except Exception as e:
            st.error(f"Error calling LLM: {e}")
            st.stop()
            
        cypher_query = build_cypher(params)
        results = execute_cypher(cypher_query)
        ranked_results = rank_candidates(results, params, driver)
        
    if ranked_results:
        for r in ranked_results:
            name = r.get('Name', 'Unknown Name')
            age = r.get('Age', 'N/A')
            craft = r.get('Craft', 'Craft Not Specified')
            region = r.get('Region', 'Region Not Specified')
            bio = r.get('Bio', '')
            
            # Format the card HTML
            card_html = f"""
            <div class="profile-card">
                <div class="profile-header">
                    <h3 class="profile-name">{name}</h3>
                    <div class="profile-meta">
                        <span class="meta-item">{craft.title()}</span> •
                        <span class="meta-item">{region.title()}</span> •
                        <span class="meta-item">{age} years old</span>
                    </div>
                </div>
                <div class="profile-bio">
                    {bio}
                </div>
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)
    else:
        st.info("No professionals found matching your criteria.")
