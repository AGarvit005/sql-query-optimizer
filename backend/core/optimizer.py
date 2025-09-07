from typing import List, Dict, Any
import sqlparse
from sqlparse.sql import Where, Comparison, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML

from backend.core.parser import SQLParser

class QueryOptimizer:
    """
    Applies heuristic-based rules to suggest query rewrites for better performance.
    """

    def __init__(self, parser: SQLParser):
        """
        Initializes the optimizer with a parsed query object.

        Args:
            parser (SQLParser): An instance of SQLParser.
        """
        self.parser = parser
        self.parsed = parser.parsed
        self.raw_sql = parser.raw_sql

    def _check_union_all_suggestion(self) -> List[Dict[str, Any]]:
        """
        Checks for the UNION keyword and suggests replacing it with UNION ALL.
        """
        suggestions = []
        # We iterate through the raw tokens of the entire statement
        for i, token in enumerate(self.parsed.tokens):
            # Find a UNION keyword
            if token.match(Keyword, 'UNION'):
                # Check if the next non-whitespace token is 'ALL'
                next_token = self.parsed.token_next(i)[1]
                if not (next_token and next_token.match(Keyword, 'ALL')):
                    # We found a UNION that is not a UNION ALL
                    
                    # To create the suggestion, we can do a simple string replace.
                    # A more robust engine would rebuild the query from tokens.
                    suggested_sql = self.raw_sql.replace('UNION', 'UNION ALL', 1)

                    suggestions.append({
                        "type": "REPLACE_UNION_WITH_UNION_ALL",
                        "suggested_sql": suggested_sql,
                        "reason": (
                            "If duplicate removal is not needed, `UNION ALL` is faster as it avoids a sort/hash operation."
                        )
                    })
                    # For simplicity, we only suggest this once per query.
                    break
        return suggestions

    def _check_subquery_to_join(self) -> List[Dict[str, Any]]:
        """
        Detects an "IN" clause with a subquery and suggests rewriting it as a JOIN.
        Example: SELECT ... WHERE user_id IN (SELECT id FROM ...)
        """
        suggestions = []
        where_clause = next((t for t in self.parsed.tokens if isinstance(t, Where)), None)
        if not where_clause:
            return suggestions

        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                # Check if the comparison is an 'IN' operator
                is_in_clause = any(t.normalized == 'IN' for t in token.tokens)
                # Check if it contains a subquery (SELECT within parenthesis)
                subquery_token = next((t for t in token.tokens if isinstance(t, Parenthesis)), None)
                
                if is_in_clause and subquery_token:
                    # Check if the parenthesis contains a SELECT statement
                    has_select = any(t.match(DML, 'SELECT') for t in subquery_token.tokens)
                    if has_select:
                        suggestions.append({
                            "type": "REWRITE_SUBQUERY_TO_JOIN",
                            # A full programmatic rewrite is very complex. For the suggestion,
                            # we will provide a template and explanation.
                            "suggested_sql": "-- Example Rewrite:\nSELECT t1.*\nFROM table1 t1\nJOIN table2 t2 ON t1.column = t2.column;",
                            "reason": "Found a subquery in an `IN` clause. Rewriting this as a `JOIN` is often significantly more performant as it allows the database planner to create a better execution strategy."
                        })
                        # Stop after finding the first instance for simplicity
                        return suggestions
        return suggestions

    def suggest_rewrites(self) -> List[Dict[str, Any]]:
        """
        Runs all available optimization checks and returns a list of suggestions.
        """
        all_suggestions = []
        
        # Rule 1: Check for UNION vs UNION ALL
        all_suggestions.extend(self._check_union_all_suggestion())
        
        # Rule 2: Check for Subquery in IN clause
        all_suggestions.extend(self._check_subquery_to_join())
        
        return all_suggestions