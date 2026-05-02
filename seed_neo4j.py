import json
import os
import glob
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def normalize_name(name):
    if not name:
        return None
    return " ".join(str(name).strip().lower().split())

def setup_constraints(session):
    queries = [
        "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (n:User) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT craft_name IF NOT EXISTS FOR (n:Craft) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT location_name IF NOT EXISTS FOR (n:Location) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT project_name IF NOT EXISTS FOR (n:Project) REQUIRE n.title IS UNIQUE",
        "CREATE CONSTRAINT banner_name IF NOT EXISTS FOR (n:Banner) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT platform_name IF NOT EXISTS FOR (n:Platform) REQUIRE n.name IS UNIQUE",
        "CREATE CONSTRAINT edu_name IF NOT EXISTS FOR (n:EducationTraining) REQUIRE n.name IS UNIQUE"
    ]
    for q in queries:
        session.run(q)

def process_data(session, profiles):
    # Setup sets for simple lookup nodes
    crafts = set()
    locations = set()
    platforms = set()
    banners = set()
    projects = {}
    edus = set()
    
    # Phase 2 & 3: Extraction and Normalization
    for p in profiles:
        prof_info = p.get("professional_info", {})
        
        # Crafts
        if prof_info.get("primary_craft"):
            crafts.add(normalize_name(prof_info["primary_craft"]))
        for c in prof_info.get("secondary_crafts", []):
            crafts.add(normalize_name(c))
        if prof_info.get("subcraft"):
            crafts.add(normalize_name(prof_info["subcraft"]))
            
        # Locations
        loc = p.get("location", {})
        if loc.get("current_city"):
            locations.add(normalize_name(loc["current_city"]))
            
        wtt = loc.get("willing_to_travel")
        if isinstance(wtt, list):
            for w in wtt:
                locations.add(normalize_name(w))
                
        # Platforms
        for ws in p.get("work_samples", []):
            url = ws.get("url") if isinstance(ws, dict) else ws
            if url and isinstance(url, str) and "." in url:
                plat = url.split(".")[0].split("/")[-1] 
                platforms.add(normalize_name(plat))
                
        # Banners & Projects
        for proj in p.get("projects", []):
            raw_title = proj.get("title") or proj.get("name")
            title = normalize_name(raw_title)
            if not title: continue
            banner = normalize_name(proj.get("banner"))
            projects[title] = {"title": raw_title, "year": proj.get("year"), "type": proj.get("type"), "banner": banner}
            if banner:
                banners.add(banner)
        
        for b in prof_info.get("previous_banners_worked_with", []):
            banners.add(normalize_name(b))
            
        # Education
        for e in p.get("education_training", []):
            ename = e.get("name") if isinstance(e, dict) else e
            if ename and isinstance(ename, str):
                edus.add(normalize_name(ename))
            
    # Insert Simple Lookup Nodes
    for c in crafts:
        if c: session.run("MERGE (n:Craft {name: $name})", name=c)
    for l in locations:
        if l: session.run("MERGE (n:Location {name: $name})", name=l)
    for plat in platforms:
        if plat: session.run("MERGE (n:Platform {name: $name})", name=plat)
    for b in banners:
        if b: session.run("MERGE (n:Banner {name: $name})", name=b)
    for e in edus:
        if e: session.run("MERGE (n:EducationTraining {name: $name})", name=e)
        
    for title, data in projects.items():
        session.run("""
            MERGE (p:Project {title: $norm_title})
            ON CREATE SET p.original_title = $title, p.year = $year, p.type = $type, p.tier = coalesce($tier, 3)
        """, norm_title=title, title=data.get("title"), year=data.get("year"), type=data.get("type"), tier=data.get("tier", 3))
        
    # Phase 4: Users
    for p in profiles:
        uid = p.get("id")
        if not uid: continue
        session.run("""
            MERGE (u:User {id: $id})
            SET u.name = $name, u.bio = $bio, u.gender = $gender, u.age = $age,
                u.experience_years = $exp, u.languages_spoken = $langs,
                u.looking_for = $looking, u.tags_self = $tags,
                u.verification_level = $verification_level,
                u.build = $build, u.appearance_tags = $appearance_tags
        """, id=p.get("id"), name=p.get("name"), bio=p.get("bio"),
            gender=p.get("personal_info", {}).get("gender"),
            age=p.get("personal_info", {}).get("age"),
            exp=p.get("professional_info", {}).get("experience_years"),
            langs=p.get("personal_info", {}).get("languages", []),
            looking=p.get("professional_info", {}).get("looking_for", []),
            tags=p.get("tags_self", []),
            verification_level=p.get("verification", {}).get("level") if isinstance(p.get("verification"), dict) else None,
            build=p.get("craft_specific_attributes", {}).get("physical_attributes", {}).get("build"),
            appearance_tags=p.get("craft_specific_attributes", {}).get("appearance_tags", []))
            
    # Phase 5: WorkSamples
    for p in profiles:
        for ws in p.get("work_samples", []):
            url = ws.get("url") if isinstance(ws, dict) else ws
            if url and isinstance(url, str):
                session.run("MERGE (n:WorkSample {url: $url})", url=url)
            
    # Phase 6: All Edges
    for p in profiles:
        uid = p.get("id")
        if not uid: continue
        
        # Primary Craft
        pc = normalize_name(p.get("professional_info", {}).get("primary_craft"))
        if pc:
            session.run("""
                MATCH (u:User {id: $uid}), (c:Craft {name: $cname})
                MERGE (u)-[:HAS_PRIMARY_CRAFT]->(c)
            """, uid=uid, cname=pc)
            
        # Secondary Crafts
        for sc in p.get("professional_info", {}).get("secondary_crafts", []):
            scn = normalize_name(sc)
            if scn:
                session.run("""
                    MATCH (u:User {id: $uid}), (c:Craft {name: $cname})
                    MERGE (u)-[:HAS_SECONDARY_CRAFT]->(c)
                """, uid=uid, cname=scn)
                
        # Subcraft CHILD_OF Primary Craft (heuristic for CHILD_OF)
        subcraft = normalize_name(p.get("professional_info", {}).get("subcraft"))
        if subcraft and pc:
            session.run("""
                MATCH (sub:Craft {name: $subname}), (main:Craft {name: $mainname})
                MERGE (sub)-[:CHILD_OF]->(main)
                MERGE (sub)-[:PART_OF]->(main)
            """, subname=subcraft, mainname=pc)
            
        # Trained Under (Lineage)
        trained = p.get("craft_specific_attributes", {}).get("trained_under")
        if trained:
            t_name = normalize_name(trained.split("(")[0])
            if t_name:
                session.run("""
                    MATCH (u:User {id: $uid})
                    MERGE (m:User {name: $mname})
                    MERGE (u)-[:TRAINED_UNDER]->(m)
                """, uid=uid, mname=t_name)

        # Lives In
        city = normalize_name(p.get("location", {}).get("current_city"))
        if city:
            session.run("""
                MATCH (u:User {id: $uid}), (l:Location {name: $lname})
                MERGE (u)-[:LIVES_IN]->(l)
            """, uid=uid, lname=city)
            
        # Willing to travel
        wtt = p.get("location", {}).get("willing_to_travel")
        if isinstance(wtt, list):
            for w in wtt:
                wn = normalize_name(w)
                if wn:
                    session.run("""
                        MATCH (u:User {id: $uid}), (l:Location {name: $lname})
                        MERGE (u)-[:WILLING_TO_TRAVEL_TO]->(l)
                    """, uid=uid, lname=wn)
                    
        # Education
        for e in p.get("education_training", []):
            ename = e.get("name") if isinstance(e, dict) else e
            if ename and isinstance(ename, str):
                en = normalize_name(ename)
                if en:
                    session.run("""
                        MATCH (u:User {id: $uid}), (ed:EducationTraining {name: $ename})
                        MERGE (u)-[:COMPLETED]->(ed)
                    """, uid=uid, ename=en)
                
        # Work Samples and Platforms
        for ws in p.get("work_samples", []):
            url = ws.get("url") if isinstance(ws, dict) else ws
            if url and isinstance(url, str):
                session.run("""
                    MATCH (u:User {id: $uid}), (w:WorkSample {url: $url})
                    MERGE (u)-[:SHOWCASES]->(w)
                """, uid=uid, url=url)
                
                if "." in url:
                    plat = url.split(".")[0].split("/")[-1]
                    platn = normalize_name(plat)
                    if platn:
                        session.run("""
                            MATCH (u:User {id: $uid}), (p:Platform {name: $pname})
                            MERGE (u)-[:HAS_PRESENCE_ON]->(p)
                        """, uid=uid, pname=platn)
                    
        # Projects & Banners
        for proj in p.get("projects", []):
            raw_title = proj.get("title") or proj.get("name")
            title = normalize_name(raw_title)
            role = proj.get("role")
            if title:
                session.run("""
                    MATCH (u:User {id: $uid}), (p:Project {title: $title})
                    MERGE (u)-[:CREDITED_ON {role: $role}]->(p)
                """, uid=uid, title=title, role=role)
                
            banner = normalize_name(proj.get("banner"))
            if banner and title:
                session.run("""
                    MATCH (p:Project {title: $title}), (b:Banner {name: $banner})
                    MERGE (b)-[:PRODUCED]->(p)
                """, title=title, banner=banner)
                
            # Add missing CREDITED_ON edges for collaborators
            for collab_id in proj.get("collaborators_on_platform", []):
                if title:
                    session.run("""
                        MATCH (u2:User {id: $collab_id}), (p:Project {title: $title})
                        MERGE (u2)-[:CREDITED_ON]->(p)
                    """, collab_id=collab_id, title=title)
                
        # Previous Banners worked with
        for b in p.get("professional_info", {}).get("previous_banners_worked_with", []):
            bn = normalize_name(b)
            if bn:
                session.run("""
                    MATCH (u:User {id: $uid}), (b:Banner {name: $bname})
                    MERGE (u)-[:AFFILIATED_WITH]->(b)
                """, uid=uid, bname=bn)
                
        # Endorsements
        for end in p.get("endorsements", []):
            end_id = end.get("id") or f"end_{uid}_{end.get('giver_id')}"
            giver_id = end.get("giver_id")
            project_title = normalize_name(end.get("project_title"))
            
            if end_id:
                # User -[ABOUT]-> Endorsement or Endorsement -[ABOUT]-> User?
                # It's an Endorsement ABOUT a User. And a User GAVE_ENDORSEMENT Endorsement.
                session.run("""
                    MERGE (e:Endorsement {id: $end_id})
                    ON CREATE SET e.text = $text
                """, end_id=end_id, text=end.get("text"))
                
                # ABOUT User
                session.run("""
                    MATCH (e:Endorsement {id: $end_id}), (u:User {id: $uid})
                    MERGE (e)-[:ABOUT]->(u)
                """, end_id=end_id, uid=uid)
                
                # GAVE_ENDORSEMENT User
                if giver_id:
                    session.run("""
                        MATCH (giver:User {id: $giver_id}), (e:Endorsement {id: $end_id})
                        MERGE (giver)-[:GAVE_ENDORSEMENT]->(e)
                    """, giver_id=giver_id, end_id=end_id)
                    
                # RELATES_TO Project
                if project_title:
                    session.run("""
                        MATCH (e:Endorsement {id: $end_id}), (p:Project {title: $title})
                        MERGE (e)-[:RELATES_TO]->(p)
                    """, end_id=end_id, title=project_title)
                
    # Phase 7: Derived Edges
    print("Computing derived edges...")
    session.run("""
        MATCH (u:User)-[:CREDITED_ON]->(p:Project)<-[:PRODUCED]-(b:Banner)
        WITH u, b, count(p) as credits
        // Keep threshold at >= 3. AFFILIATED_WITH represents patronage/long-term loyalty.
        // It is expected to be sparse/rare. Diluting it with a lower threshold breaks WBCE.
        WHERE credits >= 3
        MERGE (u)-[:AFFILIATED_WITH]->(b)
    """)
    
    session.run("""
        MATCH (u1:User)-[:CREDITED_ON]->(p:Project)<-[:CREDITED_ON]-(u2:User)
        WHERE u1.id < u2.id
        WITH u1, u2, count(p) as shared_projects
        MERGE (u1)-[r1:COLLABORATED_WITH]->(u2)
        SET r1.project_count = shared_projects
        MERGE (u2)-[r2:COLLABORATED_WITH]->(u1)
        SET r2.project_count = shared_projects
    """)
    
    # Phase 8: Final Indexes
    print("Creating indexes...")
    session.run("CREATE INDEX user_age IF NOT EXISTS FOR (u:User) ON (u.age)")
    session.run("CREATE INDEX user_exp IF NOT EXISTS FOR (u:User) ON (u.experience_years)")
    
    session.run("CREATE FULLTEXT INDEX user_bio_name IF NOT EXISTS FOR (n:User) ON EACH [n.name, n.bio]")
    session.run("CREATE FULLTEXT INDEX project_name_ft IF NOT EXISTS FOR (p:Project) ON EACH [p.original_title]")
    
    session.run("CREATE INDEX user_langs IF NOT EXISTS FOR (u:User) ON (u.languages_spoken)")
    session.run("CREATE INDEX user_looking_for IF NOT EXISTS FOR (u:User) ON (u.looking_for)")
    session.run("CREATE INDEX user_tags IF NOT EXISTS FOR (u:User) ON (u.tags_self)")

def main():
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    json_files = glob.glob(os.path.join(data_dir, '*.json'))
    
    profiles = []
    for f in json_files:
        with open(f, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if isinstance(data, list):
                profiles.extend(data)
            elif isinstance(data, dict):
                if "profiles" in data and isinstance(data["profiles"], list):
                    profiles.extend(data["profiles"])
                else:
                    profiles.append(data)
            
    print(f"Loaded {len(profiles)} profiles from {len(json_files)} files.")
    
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        with driver.session() as session:
            print("Setting up constraints...")
            setup_constraints(session)
            
            print("Processing data...")
            process_data(session, profiles)
            
            print("Seeding complete!")
    finally:
        driver.close()

if __name__ == "__main__":
    main()
