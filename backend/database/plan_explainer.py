# backend/database/plan_explainer.py

import json
import psycopg2
from typing import Dict, Any, List

class PostgresPlanExplainer:
    """
    Connects to a PostgreSQL database to retrieve and parse the execution plan
    of a given SQL query.
    """

    def __init__(self, db_params: Dict[str, Any]):
        """
        Initializes the explainer with database connection parameters.

        Args:
            db_params (Dict[str, Any]): Connection details for psycopg2
                                       (e.g., dbname, user, password, host, port).
        """
        self.db_params = db_params

    def _transform_plan_node(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively transforms a raw PostgreSQL plan node into our standardized format.
        """
        # Extract key metrics. Use .get() to handle missing keys gracefully.
        transformed = {
            "node_type": node.get("Node Type", "Unknown"),
            "estimated_cost": node.get("Total Cost", 0),
            "estimated_rows": node.get("Plan Rows", 0),
            "actual_time_ms": node.get("Actual Total Time", 0),
            "actual_rows": node.get("Actual Rows", 0),
            "details": [],
            "children": []
        }

        # Add any other interesting details
        if "Join Filter" in node:
            transformed["details"].append(f"Join Filter: {node['Join Filter']}")
        if "Hash Cond" in node:
            transformed["details"].append(f"Hash Cond: {node['Hash Cond']}")
        if "Filter" in node:
            transformed["details"].append(f"Filter: {node['Filter']}")
        if "Index Cond" in node:
            transformed["details"].append(f"Index Cond: {node['Index Cond']}")

        # Recursively transform child nodes
        if "Plans" in node:
            for child_node in node["Plans"]:
                transformed["children"].append(self._transform_plan_node(child_node))

        return transformed

    def get_plan(self, sql_query: str) -> Dict[str, Any]:
        """
        Retrieves and parses the execution plan for a SQL query.

        Args:
            sql_query (str): The SQL query to explain.

        Returns:
            A standardized dictionary representing the query plan tree.
        """
        # The EXPLAIN command asks PostgreSQL for the execution plan in JSON format.
        # ANALYZE runs the query, providing actual execution times and row counts.
        explain_query = f"EXPLAIN (FORMAT JSON, ANALYZE, COSTS, BUFFERS) {sql_query}"
        
        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(explain_query)
                    # The result of EXPLAIN is always a single row with a single column
                    result = cursor.fetchone()
                    if not result:
                        raise ValueError("Query plan could not be generated.")
                    
                    # The result is a list containing one JSON string
                    raw_plan = result[0][0]
                    
                    # Transform the raw PG plan into our clean, standard format
                    return self._transform_plan_node(raw_plan["Plan"])

        except psycopg2.Error as e:
            print(f"Database error: {e}")
            # In a real app, you'd raise a custom exception here
            return {"error": str(e)}

# --- Example Usage ---

def setup_test_schema(conn):
    """A helper function to create and populate tables for the example."""
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS orders, users;")
        cursor.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);")
        cursor.execute("CREATE TABLE orders (order_id SERIAL PRIMARY KEY, user_id INT, amount INT);")
        
        # Insert data
        cursor.execute("INSERT INTO users (name) SELECT 'user_' || s FROM generate_series(1, 1000) s;")
        cursor.execute("INSERT INTO orders (user_id, amount) SELECT s, s*10 FROM generate_series(1, 1000) s;")
    conn.commit()
    print("✅ Test schema created and populated.")

if __name__ == '__main__':
    from pprint import pprint

    # --- IMPORTANT: CONFIGURE YOUR POSTGRES CONNECTION HERE ---
    PG_CONNECTION_PARAMS = {
        "dbname": "testdb",
        "user": "postgres",
        "password": "garvit005",
        "host": "localhost",
        "port": "5432"
    }

    # 1. Setup a temporary schema with data for our test query
    try:
        with psycopg2.connect(**PG_CONNECTION_PARAMS) as conn:
            setup_test_schema(conn)
    except psycopg2.OperationalError as e:
        print(f"🚨 Could not connect to PostgreSQL. Please check your connection details in PG_CONNECTION_PARAMS.")
        print(f"   Error: {e}")
        exit()

    # 2. Define the query we want to analyze
    sample_query = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id > 500;"

    # 3. Get the standardized execution plan
    explainer = PostgresPlanExplainer(PG_CONNECTION_PARAMS)
    query_plan = explainer.get_plan(sample_query)

    print("\n--- 🌳 Standardized Query Plan (JSON for Frontend) ---")
    pprint(query_plan)
    print("-------------------------------------------------------")