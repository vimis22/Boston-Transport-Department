#!/usr/bin/env python3
"""Example script for querying Trino/Hive metadata via REST API"""

from typing import Any, Optional, Dict, List
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import os

# Suppress SSL warnings when verify_ssl=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Try to import readline for better input handling (arrow keys, history)
try:
    import readline
    READLINE_AVAILABLE = True
except ImportError:
    # readline not available (e.g., on Windows)
    READLINE_AVAILABLE = False

# Set up readline history file
if READLINE_AVAILABLE:
    history_file = os.path.expanduser("~/.trino_repl_history")
    try:
        readline.read_history_file(history_file)
        # Limit history to 1000 lines
        readline.set_history_length(1000)
    except FileNotFoundError:
        pass  # First run, no history file yet
    
    # Enable tab completion (optional, can be customized)
    readline.parse_and_bind("tab: complete")

class TrinoClient:
    """Utility class for querying Trino (and Hive metadata) via REST API"""
    
    def __init__(
        self,
        base_url: str = "https://localhost:8443",
        user: str = "trino",
        timeout: int = 30,
        verify_ssl: bool = False,
    ):
        """
        Initialize Trino REST API client.
        
        Args:
            base_url: Base URL for Trino coordinator (default: https://localhost:8443)
            user: Trino user name (default: trino)
            timeout: Request timeout in seconds (default: 30)
            verify_ssl: Whether to verify SSL certificates (default: False)
        """
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def execute_query(
        self,
        sql: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        max_wait_seconds: int = 300,
        poll_interval: float = 0.5,
    ) -> Dict[str, Any]:
        """
        Execute a SQL query and wait for results.
        
        Args:
            sql: SQL query to execute
            catalog: Optional catalog name (e.g., 'hive')
            schema: Optional schema name (e.g., 'default')
            max_wait_seconds: Maximum time to wait for query completion (default: 300)
            poll_interval: Time between polling attempts in seconds (default: 0.5)
        
        Returns:
            Dictionary containing query results with keys:
            - 'columns': List of column names
            - 'data': List of rows (each row is a list of values)
            - 'stats': Query statistics
            - 'warnings': List of warnings
        """
        # Submit query
        url = f"{self.base_url}/v1/statement"
        headers = {
            "X-Trino-User": self.user,
            "Content-Type": "text/plain",
        }
        
        if catalog:
            headers["X-Trino-Catalog"] = catalog
        if schema:
            headers["X-Trino-Schema"] = schema
        
        response = self.session.post(
            url,
            data=sql,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        response.raise_for_status()
        
        initial_response = response.json()
        next_uri = initial_response.get("nextUri")
        
        if not next_uri:
            raise ValueError("No nextUri in response. Query may have failed immediately.")
        
        # Poll for results
        start_time = time.time()
        while next_uri:
            if time.time() - start_time > max_wait_seconds:
                raise TimeoutError(f"Query did not complete within {max_wait_seconds} seconds")
            
            response = self.session.get(
                next_uri,
                headers={"X-Trino-User": self.user},
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            
            result = response.json()
            state = result.get("stats", {}).get("state", "UNKNOWN")
            
            if state == "FINISHED":
                # Extract data
                columns = [col["name"] for col in result.get("columns", [])]
                data = result.get("data", [])
                
                return {
                    "columns": columns,
                    "data": data,
                    "stats": result.get("stats", {}),
                    "warnings": result.get("warnings", []),
                }
            elif state in ["FAILED", "CANCELED"]:
                error = result.get("error", {})
                error_msg = error.get("message", "Unknown error")
                raise RuntimeError(f"Query failed: {error_msg}")
            
            # Query still running, wait and poll again
            time.sleep(poll_interval)
            next_uri = result.get("nextUri")
        
        raise RuntimeError("Query did not complete - no more nextUri")
    
    def show_catalogs(self) -> List[str]:
        """List all available catalogs"""
        result = self.execute_query("SHOW CATALOGS")
        return [row[0] for row in result["data"]]
    
    def show_schemas(self, catalog: str = "hive") -> List[str]:
        """List all schemas in a catalog"""
        result = self.execute_query(f"SHOW SCHEMAS FROM {catalog}")
        return [row[0] for row in result["data"]]
    
    def show_tables(self, catalog: str = "hive", schema: str = "default") -> List[str]:
        """List all tables in a schema"""
        result = self.execute_query(f"SHOW TABLES FROM {catalog}.{schema}")
        return [row[0] for row in result["data"]]
    
    def describe_table(self, catalog: str = "hive", schema: str = "default", table: str = "") -> Dict[str, Any]:
        """Describe a table structure"""
        result = self.execute_query(f"DESCRIBE {catalog}.{schema}.{table}")
        columns = []
        for row in result["data"]:
            columns.append({
                "column": row[0],
                "type": row[1],
                "extra": row[2] if len(row) > 2 else None,
                "comment": row[3] if len(row) > 3 else None,
            })
        return {
            "columns": columns,
            "stats": result["stats"],
        }
    
    def query_table(
        self,
        table: str,
        catalog: str = "hive",
        schema: str = "default",
        limit: Optional[int] = None,
        where: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Query data from a table.
        
        Args:
            table: Table name
            catalog: Catalog name (default: hive)
            schema: Schema name (default: default)
            limit: Optional row limit
            where: Optional WHERE clause (without the WHERE keyword)
        
        Returns:
            Dictionary with 'columns' and 'data' keys
        """
        sql = f"SELECT * FROM {catalog}.{schema}.{table}"
        
        if where:
            sql += f" WHERE {where}"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.execute_query(sql)
    
    def get_table_info(self, catalog: str = "hive", schema: str = "default", table: str = "") -> Dict[str, Any]:
        """Get comprehensive information about a table"""
        info = {
            "catalog": catalog,
            "schema": schema,
            "table": table,
        }
        
        try:
            info["description"] = self.describe_table(catalog, schema, table)
        except Exception as e:
            info["description_error"] = str(e)
        
        try:
            # Try to get row count (may be slow for large tables)
            count_result = self.execute_query(f"SELECT COUNT(*) FROM {catalog}.{schema}.{table}")
            info["row_count"] = count_result["data"][0][0] if count_result["data"] else None
        except Exception as e:
            info["row_count_error"] = str(e)
        
        return info




def format_table(columns: List[str], data: List[List[Any]], max_width: int = 80) -> str:
    """Format query results as a simple table"""
    if not columns:
        return "(no columns)"
    
    if not data:
        return "(no rows)"
    
    # Calculate column widths
    col_widths = [len(str(col)) for col in columns]
    for row in data:
        for i, val in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(val)))
    
    # Limit column widths to prevent overflow
    total_width = sum(col_widths) + (len(columns) - 1) * 3  # 3 for " | "
    if total_width > max_width:
        scale = max_width / total_width
        col_widths = [int(w * scale) for w in col_widths]
    
    # Build header
    header = " | ".join(str(col).ljust(col_widths[i])[:col_widths[i]] 
                       for i, col in enumerate(columns))
    separator = "-" * len(header)
    
    # Build rows
    rows = []
    for row in data:
        row_str = " | ".join(str(val).ljust(col_widths[i])[:col_widths[i]] 
                            for i, val in enumerate(row))
        rows.append(row_str)
    
    return "\n".join([header, separator] + rows)


def is_sql_complete(sql: str) -> bool:
    """
    Check if SQL statement appears complete.
    Simple heuristic: checks for balanced quotes, parentheses, and semicolon.
    """
    sql = sql.strip()
    
    # Empty or just whitespace
    if not sql:
        return False
    
    # Commands starting with . are always single-line
    if sql.startswith("."):
        return True
    
    # Check for balanced parentheses
    paren_count = 0
    in_single_quote = False
    in_double_quote = False
    in_block_comment = False
    
    i = 0
    while i < len(sql):
        char = sql[i]
        next_char = sql[i + 1] if i + 1 < len(sql) else None
        
        # Handle block comments
        if not in_single_quote and not in_double_quote:
            if char == '/' and next_char == '*':
                in_block_comment = True
                i += 2
                continue
            elif char == '*' and next_char == '/' and in_block_comment:
                in_block_comment = False
                i += 2
                continue
        
        if in_block_comment:
            i += 1
            continue
        
        # Handle line comments
        if not in_single_quote and not in_double_quote:
            if char == '-' and next_char == '-':
                # Rest of line is comment, but we still need to check balance
                break
        
        # Handle quotes
        if char == "'" and not in_double_quote:
            # Check for escaped quote
            if i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        
        # Count parentheses (only when not in quotes)
        if not in_single_quote and not in_double_quote:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
        
        i += 1
    
    # If we're in a quote or comment, statement is incomplete
    if in_single_quote or in_double_quote or in_block_comment:
        return False
    
    # If parentheses are unbalanced, statement is incomplete
    if paren_count != 0:
        return False
    
    # If statement ends with semicolon, it's complete
    if sql.rstrip().endswith(';'):
        return True
    
    # For statements without semicolon, check if they could continue
    sql_clean = sql.strip().upper()
    
    # If it ends with a comma, it's likely incomplete
    if sql.rstrip().endswith(','):
        return False
    
    # Check if statement ends with keywords that suggest continuation
    incomplete_endings = ['AND', 'OR', 'WHERE', 'FROM', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 
                         'FULL', 'OUTER', 'ON', 'GROUP', 'ORDER', 'HAVING', 'WITH',
                         'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']
    
    # If it ends with an incomplete keyword, it's incomplete
    for ending in incomplete_endings:
        if sql_clean.endswith(ending):
            return False
    
    # For SELECT statements, check if they could have WHERE, ORDER BY, etc.
    # A SELECT is incomplete if it doesn't end with semicolon and could have clauses
    if sql_clean.startswith('SELECT'):
        # Check if it has common SELECT clauses that might follow
        # If the last significant word is FROM and there's no WHERE/ORDER/GROUP, might continue
        words = sql_clean.split()
        if 'FROM' in words:
            from_idx = words.index('FROM')
            after_from = words[from_idx + 1:]
            # If FROM is the last significant content, might have WHERE/ORDER BY coming
            # But if we already have WHERE, ORDER BY, GROUP BY, etc., it's more likely complete
            has_where = 'WHERE' in after_from
            has_order = 'ORDER' in after_from
            has_group = 'GROUP' in after_from
            has_having = 'HAVING' in after_from
            has_limit = 'LIMIT' in after_from
            
            # If FROM is near the end and no clauses follow, might be incomplete
            # But this is tricky - let's be conservative and only mark incomplete
            # if it clearly looks incomplete (ends with FROM or a table name with no clauses)
            if not (has_where or has_order or has_group or has_having or has_limit):
                # Could still have WHERE/ORDER BY, but without semicolon, assume complete
                # unless it ends with FROM
                if sql_clean.rstrip().endswith('FROM'):
                    return False
    
    # Otherwise, assume it's complete (Trino doesn't require semicolons)
    # But if it's a single line and doesn't end with semicolon, be more cautious
    line_count = sql.count('\n') + 1
    if line_count == 1 and not sql.rstrip().endswith(';'):
        # Single line without semicolon - could be incomplete, but also could be complete
        # Let's allow it to be complete (user can press Enter twice or add semicolon)
        pass
    
    return True


def read_multiline_input(prompt: str = "trino> ", continuation_prompt: str = "... ") -> str:
    """
    Read multiline input until SQL statement is complete.
    Supports arrow key navigation and history via readline.
    """
    lines = []
    
    while True:
        try:
            if lines:
                current_prompt = continuation_prompt
            else:
                current_prompt = prompt
            
            # Use readline if available for arrow key support
            if READLINE_AVAILABLE:
                line = input(current_prompt)
            else:
                line = input(current_prompt)
            
            # Handle empty line
            if not line.strip() and not lines:
                return ""
            
            lines.append(line)
            full_input = "\n".join(lines)
            
            # Check if statement is complete
            if is_sql_complete(full_input):
                # Save to history if readline is available
                if READLINE_AVAILABLE and full_input.strip():
                    readline.add_history(full_input.strip())
                return full_input.strip()
            
        except KeyboardInterrupt:
            # On Ctrl+C, clear the input and start over
            print("\n(Cancelled. Starting new statement.)")
            return ""
        except EOFError:
            raise


def print_help():
    """Print help message"""
    print("""
Commands:
  <SQL>              Execute a SQL query (multiline supported - paste entire queries)
  .help              Show this help message
  .exit, .quit       Exit the REPL
  .catalogs          Show available catalogs
  .schemas [catalog] Show schemas (default: hive)
  .tables [catalog.schema] Show tables (default: hive.default)
  .columns [catalog.schema.table] Show columns for a table
  .info              Show useful information_schema queries
  
Multiline Support:
  - Paste entire multi-line SQL queries at once (recommended)
  - Or type line by line - REPL will automatically detect when statement is complete
  - End with semicolon (;) or just press Enter after complete statement
  - Press Ctrl+C to cancel current input and start over
  - Note: Once a query executes, you cannot continue it - type the full query again
  
Examples:
  SHOW TABLES FROM hive.default
  SELECT * FROM hive.default.students LIMIT 10
  .tables hive.default
  .columns hive.default.students
  
Information Schema Queries:
  SELECT * FROM hive.information_schema.tables WHERE table_schema = 'default'
  SELECT * FROM hive.information_schema.columns WHERE table_name = 'students'
  SELECT table_schema, table_name FROM hive.information_schema.tables
  
Note: If Spark created tables that Trino can't see, register them with:
  CREATE TABLE hive.default.students ( name VARCHAR(64),  age INT, gpa DECIMAL(3, 2) ) WITH ( format = 'PARQUET', external_location = 'hdfs://hdfs-cluster-namenode-default:9820/user/hive/warehouse/students')
""")


def main():
    """Trino REPL"""
    # Initialize client
    client = TrinoClient(
        base_url="https://localhost:8443",
        user="hive",
        verify_ssl=False,
    )
    
    print("=" * 60)
    print("Trino REPL - Query Hive metadata via REST API")
    print("=" * 60)
    print("Type '.help' for commands, '.exit' to quit")
    print("Multiline SQL supported - paste multi-line queries, end with semicolon or newline")
    if READLINE_AVAILABLE:
        print("Arrow keys: ↑/↓ for history, ←/→ for cursor movement")
    print()
    
    try:
        while True:
            try:
                # Read multiline input
                line = read_multiline_input()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Handle commands
                if line.startswith("."):
                    cmd = line.split()[0]
                    
                    if cmd in [".exit", ".quit"]:
                        print("Goodbye!")
                        break
                    elif cmd == ".help":
                        print_help()
                    elif cmd == ".catalogs":
                        try:
                            catalogs = client.show_catalogs()
                            print("\n".join(f"  {c}" for c in catalogs))
                        except Exception as e:
                            print(f"Error: {e}")
                    elif cmd == ".schemas":
                        parts = line.split()
                        catalog = parts[1] if len(parts) > 1 else "hive"
                        try:
                            schemas = client.show_schemas(catalog=catalog)
                            print("\n".join(f"  {s}" for s in schemas))
                        except Exception as e:
                            print(f"Error: {e}")
                    elif cmd == ".tables":
                        parts = line.split()
                        if len(parts) > 1:
                            catalog_schema = parts[1].split(".")
                            catalog = catalog_schema[0]
                            schema = catalog_schema[1] if len(catalog_schema) > 1 else "default"
                        else:
                            catalog = "hive"
                            schema = "default"
                        try:
                            tables = client.show_tables(catalog=catalog, schema=schema)
                            if tables:
                                print("\n".join(f"  {t}" for t in tables))
                            else:
                                print("  (no tables found)")
                        except Exception as e:
                            print(f"Error: {e}")
                    elif cmd == ".columns":
                        parts = line.split()
                        if len(parts) < 2:
                            print("Usage: .columns [catalog.schema.table]")
                            print("Example: .columns hive.default.students")
                        else:
                            table_path = parts[1].split(".")
                            if len(table_path) == 3:
                                catalog, schema, table = table_path
                            elif len(table_path) == 2:
                                catalog = "hive"
                                schema, table = table_path
                            else:
                                catalog = "hive"
                                schema = "default"
                                table = table_path[0]
                            try:
                                result = client.execute_query(
                                    f"SELECT column_name, data_type, is_nullable "
                                    f"FROM {catalog}.information_schema.columns "
                                    f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
                                    f"ORDER BY ordinal_position"
                                )
                                if result["data"]:
                                    print(format_table(result["columns"], result["data"]))
                                else:
                                    print(f"  (no columns found for {catalog}.{schema}.{table})")
                            except Exception as e:
                                print(f"Error: {e}")
                    elif cmd == ".info":
                        print("""
Useful Information Schema Queries:

1. List all tables in a schema:
   SELECT table_schema, table_name, table_type 
   FROM hive.information_schema.tables 
   WHERE table_schema = 'default'

2. List all columns for a table:
   SELECT column_name, data_type, is_nullable, column_default
   FROM hive.information_schema.columns 
   WHERE table_schema = 'default' AND table_name = 'students'
   ORDER BY ordinal_position

3. Find all tables with a specific column:
   SELECT table_schema, table_name 
   FROM hive.information_schema.columns 
   WHERE column_name = 'name'

4. Get table and column info together:
   SELECT t.table_schema, t.table_name, c.column_name, c.data_type
   FROM hive.information_schema.tables t
   JOIN hive.information_schema.columns c 
     ON t.table_schema = c.table_schema AND t.table_name = c.table_name
   WHERE t.table_schema = 'default'
   ORDER BY t.table_name, c.ordinal_position

5. List all schemas with table counts:
   SELECT table_schema, COUNT(*) as table_count
   FROM hive.information_schema.tables
   GROUP BY table_schema
   ORDER BY table_schema
""")
                    else:
                        print(f"Unknown command: {cmd}. Type '.help' for help.")
                    continue
                
                # Check if line starts with continuation keywords (common mistake)
                line_upper = line.strip().upper()
                continuation_keywords = ['WHERE', 'AND', 'OR', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET']
                if any(line_upper.startswith(kw) for kw in continuation_keywords):
                    print(f"Error: '{line.split()[0]}' cannot start a statement.")
                    print("Hint: Paste the entire query at once, or continue on the same input.")
                    print("      The REPL will automatically detect multiline queries.")
                    continue
                
                # Execute SQL query
                try:
                    result = client.execute_query(line)
                    
                    # Display results
                    if result["columns"]:
                        print(format_table(result["columns"], result["data"]))
                        print(f"\n({len(result['data'])} row(s))")
                    else:
                        print("Query executed successfully (no results)")
                    
                    # Show warnings if any
                    if result.get("warnings"):
                        for warning in result["warnings"]:
                            print(f"Warning: {warning}")
                            
                except Exception as e:
                    print(f"Error: {e}")
                    
            except KeyboardInterrupt:
                print("\nUse '.exit' to quit")
            except EOFError:
                print("\nGoodbye!")
                break
    finally:
        # Save history on exit
        if READLINE_AVAILABLE:
            try:
                readline.write_history_file(history_file)
            except Exception:
                pass  # Ignore errors saving history


if __name__ == "__main__":
    main()

