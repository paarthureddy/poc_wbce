import os
import json
import streamlit as st
from neo4j import GraphDatabase
from openai import AzureOpenAI
from dotenv import load_dotenv

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
      Examples to drop: "soonish", "available", "budget", "experienced", "good", "best", "need", "someone", "like", "prefer", "based".
    - "gender": (string) "male" or "female" if the query explicitly mentions a gender (e.g., "guy", "brother", "actress"). otherwise null.
    
    Example input: "Find me a director in Hyderabad who works in mass entertainers and has worked with Mythri"
    Example output:
    {
      "craft": "director",
      "location": "hyderabad",
      "banner": "mythri",
      "keywords": ["mass", "entertainer"],
      "gender": null,
      "age_range": null,
      "physique": []
    }
    
    IMPORTANT: Do NOT repeat the input, do NOT add explanations, and do NOT produce any other text. If any field cannot be extracted, set it to null (or an empty list for "keywords" and "physique").
    """
    
    response = llm_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ]
    )
    
    # Clean up output to ensure valid JSON parsing
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
        
    # Role/context words that describe the casting need but are NOT searchable tags in the graph
    STOP_WORDS = {
        "brother", "sister", "friend", "villain", "hero", "role", "character", 
        "soonish", "available", "budget", "experienced", "good", "best", 
        "need", "someone", "like", "prefer", "based", "looking", "want", "find"
    }

    # Physique synonyms come from the physique field
    physique_kws = []
    if params.get("physique") and isinstance(params["physique"], list):
        physique_kws = params["physique"]
    elif params.get("physique") and isinstance(params["physique"], str):
        physique_kws = [params["physique"]]

    # Generic keywords (genres, vibe words) — filter out stop words
    generic_kws = [k for k in (params.get("keywords") or []) if k.lower() not in STOP_WORDS]

    all_keywords = physique_kws + generic_kws

    if all_keywords:
        lines.append("WITH DISTINCT u")
        lines.append("OPTIONAL MATCH (u)-[:CREDITED_ON]->(p_keywords:Project)")
        lines.append("WITH u, collect(p_keywords.type) as project_types")
        
        # Soft Scoring: Keywords contribute positively, but absence isn't disqualifying
        score_cases = []
        for kw in all_keywords:
            kw_clean = kw.replace("'", "\\'") # basic escaping
            score_cases.append(f"(CASE WHEN toLower(u.bio) CONTAINS toLower('{kw_clean}') THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN any(tag IN u.tags_self WHERE toLower(tag) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN any(tag IN u.appearance_tags WHERE toLower(tag) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN toLower(u.build) CONTAINS toLower('{kw_clean}') THEN 1 ELSE 0 END)")
            score_cases.append(f"(CASE WHEN any(ptype IN project_types WHERE toLower(ptype) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)")
            
        lines.append("WITH u, (" + " + ".join(score_cases) + ") AS keyword_score")
        lines.append("RETURN DISTINCT u.id AS id, u.name AS Name, u.bio AS Bio, keyword_score")
        lines.append("ORDER BY keyword_score DESC")
    else:
        lines.append("RETURN DISTINCT u.id AS id, u.name AS Name, u.bio AS Bio, 0 AS keyword_score")
        
    return "\n".join(lines)

def execute_cypher(query):
    with driver.session() as session:
        result = session.run(query)
        return [dict(record) for record in result]

# -----------------
# STREAMLIT UI
# -----------------
st.set_page_config(page_title="Talent Search POC", layout="wide")

st.title("Talent Search Engine POC")
st.markdown("Search across the Neo4j graph using natural language.")

user_query = st.text_input("Search:", placeholder="e.g. Find me a director in Hyderabad who works in mass entertainers and has worked with Mythri")

if st.button("Search") and user_query:
    with st.spinner("Decomposing prompt using Azure OpenAI..."):
        try:
            params = decompose_prompt(user_query)
        except Exception as e:
            st.error(f"Error calling LLM: {e}")
            st.stop()
            
    st.subheader("1. Prompt Decomposition")
    st.json(params)
    
    with st.spinner("Building Cypher Query..."):
        cypher_query = build_cypher(params)
        
    st.subheader("2. Generated Cypher")
    st.code(cypher_query, language="cypher")
    
    with st.spinner("Executing Graph Query..."):
        results = execute_cypher(cypher_query)
        
        st.subheader(f"3. Graph Results ({len(results)} matches)")
    if results:
        for r in results:
            score_text = f" (Score: {r.get('keyword_score', 0)})" if r.get('keyword_score', 0) > 0 else ""
            with st.expander(f"{r['Name']}{score_text}"):
                st.write(r['Bio'])
    else:
        st.warning("No matches found in the graph.")
