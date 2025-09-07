# backend/core/parser.py

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Where, Comparison, Function
from typing import List, Optional, Set, Dict, Any

class SQLParser:
    """
    Parses a raw SQL query string into a structured representation.
    
    This class leverages the sqlparse library to create an Abstract Syntax Tree (AST)
    and provides methods to extract essential query components like tables, columns,
    join conditions, and WHERE clauses.
    """

    def __init__(self, sql: str):
        """
        Initializes the parser with a SQL query.

        Args:
            sql (str): The raw SQL query string.
        """
        self.raw_sql = sql
        self.parsed = sqlparse.parse(self.raw_sql)[0]
        
        self._tables_and_aliases: Optional[Dict[str, str]] = None
        self._columns: Optional[List[str]] = None

    def _get_tables_and_aliases(self) -> Dict[str, str]:
        """
        Extracts tables and their aliases. Aliases are keys, real names are values.
        """
        if self._tables_and_aliases is not None:
            return self._tables_and_aliases

        tables = {}
        from_or_join_seen = False
        tokens = self.parsed.tokens

        for token in tokens:
            if from_or_join_seen:
                if isinstance(token, Identifier):
                    real_name = token.get_real_name()
                    alias = token.get_alias() or real_name
                    tables[alias] = real_name
                elif isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        real_name = identifier.get_real_name()
                        alias = identifier.get_alias() or real_name
                        tables[alias] = real_name
                
                if not (token.is_whitespace or token.normalized == ','):
                    from_or_join_seen = False

            if token.is_keyword and token.normalized in ('FROM', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN'):
                from_or_join_seen = True
        
        self._tables_and_aliases = tables
        return self._tables_and_aliases

    def extract_tables(self) -> List[str]:
        """Extracts all unique table names referenced in the query."""
        tables_with_aliases = self._get_tables_and_aliases()
        return sorted(list(set(tables_with_aliases.values())))

    def _recursive_extract_columns(self, token, column_set: Set[str]):
        """
        Recursively walks the token tree to find column identifiers,
        now correctly handling Function objects.
        """
        # If the token is a Function, we don't add its name (e.g., 'COUNT').
        # Instead, we recurse into its children to find columns used as arguments (e.g., 'id').
        if isinstance(token, Function):
            for sub_token in token.tokens:
                self._recursive_extract_columns(sub_token, column_set)
            return

        # An Identifier is a potential column.
        if isinstance(token, Identifier):
            # We must exclude any identifiers that are actually table names or aliases.
            all_table_references = list(self._get_tables_and_aliases().keys()) + list(self._get_tables_and_aliases().values())
            if token.get_real_name() not in all_table_references:
                column_set.add(token.get_name())

        # If the token is a group (but not a function we've already handled), recurse.
        if hasattr(token, 'tokens'):
            for sub_token in token.tokens:
                self._recursive_extract_columns(sub_token, column_set)

    def extract_columns(self) -> List[str]:
        """
        Extracts all unique column names referenced in the query.
        """
        if self._columns is not None:
            return self._columns

        columns: Set[str] = set()
        self._recursive_extract_columns(self.parsed, columns)
        self._columns = sorted(list(columns))
        return self._columns

    def extract_where_clause(self) -> Optional[str]:
        """Extracts the WHERE clause as a raw string, if it exists."""
        where_token = next((token for token in self.parsed.tokens if isinstance(token, Where)), None)
        return str(where_token) if where_token else None

    def get_query_type(self) -> str:
        """Determines the type of the SQL query (e.g., SELECT, INSERT)."""
        return self.parsed.get_type()

    def get_analysis_summary(self) -> Dict[str, Any]:
        """Provides a comprehensive summary of the parsed query."""
        return {
            "query_type": self.get_query_type(),
            "tables": self.extract_tables(),
            "columns": self.extract_columns(),
            "where_clause": self.extract_where_clause(),
        }

# --- Example Usage ---
if __name__ == '__main__':
    sample_query = """
    SELECT
        u.id,
        u.name,
        p.product_name,
        o.order_date,
        COUNT(o.id) as order_count
    FROM
        users u
    JOIN
        orders o ON u.id = o.user_id
    LEFT JOIN
        products p ON o.product_id = p.id
    WHERE
        u.registration_date > '2024-01-01' AND p.category = 'electronics';
    """

    parser = SQLParser(sample_query)
    summary = parser.get_analysis_summary()

    print("--- SQL Query Analysis (Final Correction) ---")
    print(f"Query Type: {summary['query_type']}")
    print(f"Tables Found: {summary['tables']}")
    print(f"Columns Found: {summary['columns']}")
    print(f"WHERE Clause: {summary['where_clause']}")
    print("-----------------------------------------")
