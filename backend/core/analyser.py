# backend/core/analyzer.py (Final Version)

import re
from typing import List, Dict, Any, Set, Tuple

from core.parser import SQLParser
from sqlparse.sql import Where, Comparison, Identifier, IdentifierList, Function, Parenthesis

class QueryAnalyzer:
    """
    Final, robust version. This correctly captures alias context for all clauses.
    """
    def __init__(self, parser: SQLParser):
        self.parser = parser
        self.parsed = parser.parsed

    def _recursive_find_identifiers(self, token, columns: Set[Tuple[str, str]]):
        if isinstance(token, Function):
            for sub_token in token.tokens:
                if isinstance(sub_token, Parenthesis):
                    self._recursive_find_identifiers(sub_token, columns)
            return

        if isinstance(token, Identifier):
            alias = token.get_parent_name() or 'unknown'
            column_name = token.get_name()
            columns.add((alias, column_name))

        if hasattr(token, 'tokens'):
            for sub_token in token.tokens:
                self._recursive_find_identifiers(sub_token, columns)

    def analyze_column_usage(self) -> Dict[str, List[Tuple[str, str]]]:
        usage: Dict[str, Set[Tuple[str, str]]] = {
            "where_filters": set(), "join_keys": set(),
            "order_by": set(), "group_by": set(),
        }
        is_after_on = False
        for token in self.parsed.tokens:
            if token.is_keyword and token.normalized == 'ON':
                is_after_on = True
                continue
            if is_after_on and isinstance(token, Comparison):
                self._recursive_find_identifiers(token, usage["join_keys"])
                is_after_on = False
            if isinstance(token, Where):
                self._recursive_find_identifiers(token, usage["where_filters"])
            
            # *** THIS IS THE FIX ***
            # Use the robust recursive method for ORDER BY and GROUP BY as well.
            if token.is_keyword:
                clause_key = None
                if 'ORDER BY' in token.normalized: clause_key = "order_by"
                elif 'GROUP BY' in token.normalized: clause_key = "group_by"
                if clause_key:
                    next_token = self.parsed.token_next(self.parsed.token_index(token))[1]
                    self._recursive_find_identifiers(next_token, usage[clause_key])
        
        return {key: sorted(list(cols)) for key, cols in usage.items()}

    # --- detect_anti_patterns and run_analysis methods are unchanged ---
    def detect_anti_patterns(self) -> List[Dict[str, str]]:
        anti_patterns = []
        if self.parser.get_query_type() == 'SELECT':
            if any(token.normalized == '*' for token in self.parsed.tokens):
                anti_patterns.append({"type": "SELECT_STAR", "message": "Avoid using 'SELECT *'."})
        query_type = self.parser.get_query_type()
        if query_type in ('UPDATE', 'DELETE') and not self.parser.extract_where_clause():
            anti_patterns.append({"type": "MISSING_WHERE_CLAUSE", "message": f"The {query_type} statement lacks a WHERE clause."})
        where_clause_str = self.parser.extract_where_clause()
        if where_clause_str and re.search(r'\b\w+\s*\(\s*[\w\.]+\s*\)', where_clause_str):
            anti_patterns.append({"type": "FUNCTION_ON_COLUMN_IN_WHERE", "message": "Found a function call on a column in the WHERE clause."})
        return anti_patterns

    def run_analysis(self) -> Dict[str, Any]:
        tables = self.parser.extract_tables()
        return {
            "query_type": self.parser.get_query_type(), "tables": tables,
            "anti_patterns": self.detect_anti_patterns(),
            "column_usage": self.analyze_column_usage(),
            "join_count": len(tables) - 1 if len(tables) > 0 else 0,
        }

# --- Example Usage ---
if __name__ == '__main__':
    from pprint import pprint
    sample_query = """
    SELECT u.name, p.product_name, o.order_date
    FROM users u JOIN orders o ON u.id = o.user_id
    LEFT JOIN products p ON o.product_id = p.id
    WHERE LOWER(u.name) = 'john doe' AND p.category = 'electronics'
    ORDER BY o.order_date DESC;
    """
    parser = SQLParser(sample_query)
    analyzer = QueryAnalyzer(parser)
    analysis_results = analyzer.run_analysis()
    print("--- 🔬 SQL Query Analysis Report (Definitive Correction) ---")
    pprint(analysis_results)
    print("----------------------------------------------------------")