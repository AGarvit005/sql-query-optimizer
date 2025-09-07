# backend/core/benchmarker.py

import sqlite3
import time
import pandas as pd
import numpy as np
from typing import List, Dict, Any

class PerformanceBenchmarker:
    """
    Executes original and suggested queries against a synthetic dataset
    to measure and compare their actual performance.
    """

    def __init__(self, tables: List[str], original_sql: str, suggestions: List[Dict[str, Any]]):
        """
        Initializes the benchmarker.

        Args:
            tables (List[str]): A list of tables involved in the query.
            original_sql (str): The original user-submitted SQL query.
            suggestions (List[Dict[str, Any]]): A list of rewrite suggestions from the optimizer.
        """
        self.tables = tables
        self.original_sql = original_sql
        self.suggestions = suggestions
        self.conn = None

    def _create_test_database(self):
        """Creates an in-memory SQLite database and populates it with synthetic data."""
        self.conn = sqlite3.connect(":memory:")
        self._generate_and_load_data()

    def _generate_and_load_data(self, num_rows=10000):
        """
        Generates synthetic data for the required tables and loads it into SQLite.
        This is a simplified data generator for demonstration.
        """
        cursor = self.conn.cursor()

        if 'customers_2024' in self.tables and 'customers_archive' in self.tables:
            # Data for the UNION query example
            customers_2024_df = pd.DataFrame({
                'id': range(num_rows),
                'email': [f'customer{i}@2024.com' for i in range(num_rows)]
            })
            customers_archive_df = pd.DataFrame({
                'id': range(num_rows // 2, num_rows + num_rows // 2), # Create some overlap for UNION
                'email': [f'customer{i}@archive.com' for i in range(num_rows // 2, num_rows + num_rows // 2)]
            })
            customers_2024_df.to_sql('customers_2024', self.conn, if_exists='replace', index=False)
            customers_archive_df.to_sql('customers_archive', self.conn, if_exists='replace', index=False)
            
        # Add data generation logic for other schemas (e.g., users, orders) here if needed
        # ...

        self.conn.commit()

    def _measure_execution_time(self, sql: str, runs: int = 5) -> float:
        """
        Executes a query multiple times and returns the average execution time.

        Args:
            sql (str): The SQL query to execute.
            runs (int): The number of times to run the query for a stable average.

        Returns:
            The average execution time in seconds.
        """
        if not self.conn:
            return -1.0

        total_time = 0
        cursor = self.conn.cursor()
        for _ in range(runs):
            start_time = time.perf_counter()
            cursor.execute(sql).fetchall() # .fetchall() ensures we measure time to get all data
            end_time = time.perf_counter()
            total_time += (end_time - start_time)
        
        return total_time / runs

    def run_benchmark(self) -> Dict[str, Any]:
        """
        Orchestrates the benchmarking process and returns a comparison report.
        """
        self._create_test_database()

        # Benchmark the original query
        original_avg_time = self._measure_execution_time(self.original_sql)

        benchmark_results = {
            "original_query": {
                "sql": self.original_sql,
                "avg_execution_time_ms": original_avg_time * 1000
            },
            "suggestions": []
        }

        # Benchmark each suggestion
        for suggestion in self.suggestions:
            suggested_sql = suggestion.get("suggested_sql")
            if suggested_sql:
                suggestion_avg_time = self._measure_execution_time(suggested_sql)
                
                improvement = 0
                if suggestion_avg_time > 0:
                    improvement = ((original_avg_time - suggestion_avg_time) / original_avg_time) * 100

                benchmark_results["suggestions"].append({
                    "sql": suggested_sql,
                    "avg_execution_time_ms": suggestion_avg_time * 1000,
                    "performance_improvement_percent": improvement,
                    "reason": suggestion.get("reason")
                })
        
        if self.conn:
            self.conn.close()
            
        return benchmark_results

# --- Example Usage ---
if __name__ == '__main__':
    from pprint import pprint
    from parser import SQLParser
    from optimizer import QueryOptimizer

    sample_query_with_union = """
    SELECT id, email FROM customers_2024
    UNION
    SELECT id, email FROM customers_archive;
    """

    # 1. Parse
    parser = SQLParser(sample_query_with_union)
    
    # 2. Get rewrite suggestions
    optimizer = QueryOptimizer(parser)
    rewrites = optimizer.suggest_rewrites()

    # 3. Benchmark the original query against the suggestions
    # We need the table names from the parser for the benchmarker
    tables = parser.extract_tables()
    benchmarker = PerformanceBenchmarker(tables, sample_query_with_union, rewrites)
    results = benchmarker.run_benchmark()

    print("--- 📊 Performance Benchmark Report ---")
    pprint(results)
    print("--------------------------------------")