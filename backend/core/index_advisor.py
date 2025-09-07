# backend/core/index_advisor.py (Corrected)

from typing import List, Dict, Any, Set, Tuple
import sqlparse
from sqlparse.sql import Identifier, IdentifierList

class IndexAdvisor:
    """
    Generates index recommendations based on a query analysis report.
    This version is simpler and more reliable due to improved data from the analyzer.
    """
    def __init__(self, analysis_report: Dict[str, Any]):
        self.report = analysis_report
        self.column_usage = self.report.get("column_usage", {})
        self.alias_to_table_map = self._map_aliases_to_tables()

    def _map_aliases_to_tables(self) -> Dict[str, str]:
        """
        Correctly builds a map of table aliases (e.g., 'u') to real table names (e.g., 'users').
        """
        mapping = {}
        parsed = sqlparse.parse(self.report.get("raw_sql", ""))[0] # Assuming raw_sql is passed in report
        
        # We need to add raw_sql to the report for this to work
        from_or_join_seen = False
        for token in parsed.tokens:
            if from_or_join_seen:
                if isinstance(token, Identifier):
                    mapping[token.get_alias() or token.get_real_name()] = token.get_real_name()
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        mapping[identifier.get_alias() or identifier.get_real_name()] = identifier.get_real_name()
            
            if token.is_keyword and token.normalized in ('FROM', 'JOIN', 'LEFT JOIN'):
                from_or_join_seen = True
            elif token.is_keyword:
                from_or_join_seen = False # Reset state after FROM/JOIN block
        return mapping

    def _generate_index_name(self, table: str, columns: List[str]) -> str:
        return f"idx_{table}_{'_'.join(columns)}"

    def generate_recommendations(self) -> List[Dict[str, Any]]:
        """
        Generates index recommendations using (alias, column) tuples.
        """
        recommendations = []
        # Keep track of composite recommendations to avoid suggesting single indexes for the same columns
        recommended_composite_cols: Set[Tuple[str, str]] = set()

        # Composite index recommendations (Highest Priority)
        where_filters = self.column_usage.get("where_filters", [])
        if len(where_filters) > 1:
            table_to_cols: Dict[str, List[Tuple[str, str]]] = {}
            for alias, column in where_filters:
                table = self.alias_to_table_map.get(alias)
                if table:
                    if table not in table_to_cols: table_to_cols[table] = []
                    table_to_cols[table].append((alias, column))

            for table, cols in table_to_cols.items():
                if len(cols) > 1:
                    sorted_cols = sorted([c for a, c in cols])
                    index_name = self._generate_index_name(table, sorted_cols)
                    statement = f"CREATE INDEX {index_name} ON {table} ({', '.join(sorted_cols)});"
                    recommendations.append({
                        "statement": statement,
                        "reason": f"Columns {', '.join(sorted_cols)} are used together in the WHERE clause.",
                        "impact": "High"
                    })
                    for ac_tuple in cols:
                        recommended_composite_cols.add(ac_tuple)

        # Single-column index recommendations
        usage_types = {
            "where_filters": "filtering in the WHERE clause",
            "join_keys": "a JOIN condition",
            "order_by": "sorting in the ORDER BY clause",
        }
        for usage_type, reason_text in usage_types.items():
            for alias, column in self.column_usage.get(usage_type, []):
                if (alias, column) in recommended_composite_cols:
                    continue # Skip if already part of a composite index recommendation

                table = self.alias_to_table_map.get(alias)
                if table:
                    index_name = self._generate_index_name(table, [column])
                    statement = f"CREATE INDEX {index_name} ON {table} ({column});"
                    recommendations.append({
                        "statement": statement,
                        "reason": f"Column '{column}' is used for {reason_text}.",
                        "impact": "High" if usage_type != 'order_by' else 'Medium'
                    })
        return recommendations

# --- Example Usage (You'll need to slightly modify your test harness) ---
if __name__ == '__main__':
    from pprint import pprint
    from core.parser import SQLParser
    from core.analyser import QueryAnalyzer
    # ... (assuming parser and analyzer are imported)
    sample_query = """
    SELECT u.name, p.product_name, o.order_date
    FROM users u JOIN orders o ON u.id = o.user_id
    LEFT JOIN products p ON o.product_id = p.id
    WHERE u.name = 'john doe' AND p.category = 'electronics'
    ORDER BY o.order_date DESC;
    """
    parser = SQLParser(sample_query)
    analyzer = QueryAnalyzer(parser)
    analysis_results = analyzer.run_analysis()
    analysis_results['raw_sql'] = sample_query # Add raw_sql to the report for the advisor

    advisor = IndexAdvisor(analysis_results)
    recommendations = advisor.generate_recommendations()

    print("\n--- 💡 Index Recommendations (Corrected) ---")
    if recommendations:
        # Sort for consistent output
        for rec in sorted(recommendations, key=lambda x: x['statement']):
            print(f"Impact: {rec['impact']}\n  Reason: {rec['reason']}\n  SQL: {rec['statement']}\n")
    else:
        print("No specific index recommendations for this query.")