# WBCE (Work Based Credential Engine) - POC

A powerful proof-of-concept application built to search, rank, and evaluate professionals in the entertainment industry using a knowledge graph (Neo4j) and Large Language Models (Azure OpenAI). The app features an intuitive Streamlit interface, allowing natural language queries that are intelligently decomposed into structured graph searches.

## 🚀 Features
- **Natural Language Search:** Describe the talent you're looking for (e.g., "Find me a director in Hyderabad with experience in tier 1 movies").
- **Knowledge Graph Integration:** Queries are translated into Cypher to fetch candidates based on their connections, credits, and peer endorsements.
- **AI-Powered Evaluation:** Uses Azure OpenAI to score candidates, evaluate their relevance, and provide an auto-generated justification summary for each profile.
- **Comprehensive Profiles:** View detailed facts, credits, and industry context directly within the UI.

## 🛠️ Technology Stack
- **Frontend / App Framework:** [Streamlit](https://streamlit.io/)
- **Database:** [Neo4j](https://neo4j.com/) (AuraDB / Local)
- **AI / LLM:** [Azure OpenAI](https://azure.microsoft.com/en-us/products/ai-services/openai-service) (using `gpt-5.4-nano`)
- **Backend Language:** Python 3.11+
- **Graph Query Language:** Cypher

---

## 📸 Screenshots

*(Replace these with actual screenshots of your application)*

![Search Interface](https://via.placeholder.com/800x400?text=Search+Interface)
![Candidate Evaluation](https://via.placeholder.com/800x400?text=Candidate+Evaluation)
![Structured Query JSON](https://via.placeholder.com/800x400?text=Structured+Query+JSON)

---

## 💻 Local Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/paarthureddy/poc_wbce.git
cd poc_wbce
```

### 2. Set up a Virtual Environment
```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On Mac/Linux:
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Copy the provided `.env.example` file to create your local `.env` file:
```bash
cp .env.example .env
```
Open the `.env` file and fill in your Neo4j and Azure OpenAI credentials:
```ini
NEO4J_URI=neo4j+s://your-neo4j-uri.databases.neo4j.io
NEO4J_USER=your_username
NEO4J_PASSWORD=your_password

OAI_BASE_LLM=https://your-azure-endpoint.openai.azure.com/
OAI_KEY_LLM=your_azure_api_key
OAI_VERSION=2024-12-01-preview
LLM_MODEL_NAME=gpt-5.4-nano
```

### 5. Seed the Database (Optional but recommended)
If you are running against a fresh database, run the seeding script to populate it with mock data:
```bash
python seed_neo4j.py
```

### 6. Run the Application
Launch the Streamlit server:
```bash
streamlit run app.py
```
The app should now be running at `http://localhost:8501`.

---

## ☁️ Deployment on Streamlit Cloud

To deploy this application to Streamlit Community Cloud:

1. **Push your code to GitHub** (Ensure your `.env` file is NOT pushed, it is correctly listed in `.gitignore`).
2. Go to [Streamlit Cloud](https://share.streamlit.io/) and click **New app**.
3. Select this repository, branch (`main`), and the main file path (`app.py`).
4. **CRITICAL STEP - Configure Secrets:** 
   Streamlit Cloud does not read the `.env` file. You must add your credentials in the dashboard.
   - Click **Advanced settings...** before deploying, or go to your app's **Settings -> Secrets** after deploying.
   - Paste the following TOML configuration (replace with your real credentials):

   ```toml
   NEO4J_URI = "neo4j+s://ba06fa43.databases.neo4j.io"
   NEO4J_USER = "ba06fa43"
   NEO4J_PASSWORD = "your_actual_password_here"
   
   OAI_BASE_LLM = "https://openaiservices-dev.openai.azure.com/"
   OAI_KEY_LLM = "your_actual_azure_key_here"
   OAI_VERSION = "2024-12-01-preview"
   LLM_MODEL_NAME = "gpt-5.4-nano"
   ```
5. Click **Deploy!**

### Troubleshooting Deployed App
- **Database Connection Errors:** The app has graceful error handling. If you see a database connection error banner at startup, it means your `NEO4J_*` secrets are missing or incorrect in the Streamlit Cloud dashboard.
- **Search Errors:** If searching throws an error, the specific database or ranking error message will be printed in the UI.

---

## 📁 Project Structure

- `app.py`: Main Streamlit application entry point and UI layout.
- `lib/pipeline/`: Core logic for natural language decomposition, query building, ranking, and justifications.
- `seed_neo4j.py`: Script to populate Neo4j with mock professional nodes and relationships.
- `data/`: Contains mock JSON data used for seeding the knowledge graph.
- `layered_schema.html`: An interactive visualization of the graph schema.
- `.streamlit/config.toml`: Custom theme configuration for the Streamlit UI.
