import os
import json
import joblib
import pandas as pd
from typing import Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Import all our core logic components
from core.parser import SQLParser
from core.analyser import QueryAnalyzer
from core.index_advisor import IndexAdvisor
from core.optimizer import QueryOptimizer
from database.plan_explainer import PostgresPlanExplainer

# --- Load the ML Model on Startup ---
# Use the absolute path inside the container
model_path = '/app/ml/model.joblib'
try:
    model = joblib.load(model_path)
    print("✅ ML model loaded successfully.")
except FileNotFoundError:
    model = None
    print("⚠️ ML model not found. Running without ML predictions.")

# Initialize the FastAPI app
app = FastAPI()

# --- CORS Middleware ---
# Allows our React frontend to communicate with this backend
origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic Models ---
class SQLQueryRequest(BaseModel):
    sql: str

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"message": "SQL Optimizer API is running."}


@app.post("/api/v1/analyze-query")
def analyze_query(request: SQLQueryRequest) -> Dict[str, Any]:
    """
    This endpoint receives a SQL query and returns a full analysis,
    including index recommendations and optimization suggestions.
    """
    sql_query = request.sql
    
    # 1. Parse and Analyze
    parser = SQLParser(sql_query)
    analyzer = QueryAnalyzer(parser)
    analysis_report = analyzer.run_analysis()
    analysis_report['raw_sql'] = sql_query
    
    # 2. Get Index Recommendations
    advisor = IndexAdvisor(analysis_report)
    index_recommendations = advisor.generate_recommendations()
    
    # 3. Get Query Rewrites
    optimizer = QueryOptimizer(parser)
    rewrite_suggestions = optimizer.suggest_rewrites()
    
    # 4. Add ML Predictions (if model is loaded)
    if model:
        features = {
            'join_count': analysis_report.get('join_count', 0),
            'where_clause_count': len(analysis_report.get('column_usage', {}).get('where_filters', []))
        }
        feature_df = pd.DataFrame([features])
        
        for rec in index_recommendations:
            prediction = model.predict(feature_df)[0]
            rec['ml_predicted_impact'] = prediction

    # 5. Bundle and return all results
    return {
        "analysis": analysis_report,
        "index_recommendations": index_recommendations,
        "rewrite_suggestions": rewrite_suggestions,
    }


@app.post("/api/v1/explain-plan")
def get_query_plan(request: SQLQueryRequest) -> Dict[str, Any]:
    """
    Receives a SQL query and returns its execution plan from PostgreSQL.
    """
    PG_CONNECTION_PARAMS = {
        "dbname": os.environ.get("POSTGRES_DB"),
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "host": "db",  # Use the service name from docker-compose
        "port": "5432"
    }
    
    explainer = PostgresPlanExplainer(PG_CONNECTION_PARAMS)
    plan = explainer.get_plan(request.sql)
    return {"plan": plan}