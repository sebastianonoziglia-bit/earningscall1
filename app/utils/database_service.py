"""
Database service module for connecting to PostgreSQL
"""
import os
import psycopg
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_connection():
    """
    Get a connection to the PostgreSQL database
    
    Returns:
        psycopg.Connection: Database connection object
    """
    try:
        # Get database URL from environment variables
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        # Create a connection to the database
        conn = psycopg.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

# Create an alias for backward compatibility
get_db_connection = get_connection

def execute_query(query, params=None, fetch=True):
    """
    Execute a SQL query and optionally return results
    
    Args:
        query (str): SQL query to execute
        params (tuple, optional): Parameters for the query
        fetch (bool, optional): Whether to fetch and return results
        
    Returns:
        list: Query results (if fetch=True)
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        if fetch:
            results = cursor.fetchall()
        else:
            conn.commit()
            results = None
            
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise

def get_table_schema(table_name):
    """
    Get schema information for a specific table
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        list: Column information for the table
    """
    try:
        query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable
        FROM 
            information_schema.columns
        WHERE 
            table_name = %s
        ORDER BY 
            ordinal_position
        """
        
        results = execute_query(query, (table_name,))
        return results
    except Exception as e:
        logger.error(f"Error getting schema for table {table_name}: {str(e)}")
        return []

def table_exists(table_name):
    """
    Check if a table exists in the database
    
    Args:
        table_name (str): Name of the table to check
        
    Returns:
        bool: True if the table exists, False otherwise
    """
    try:
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
        """
        
        results = execute_query(query, (table_name,))
        return results[0][0]
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {str(e)}")
        return False

def get_forward_signals(
    company: str = None,
    year: int = None,
    quarter: str = None,
    limit: int = 10,
    min_score: float = 0.3,
) -> list:
    """
    Query forward_signals table from the SQLite intelligence DB.
    If company is None: returns top signals across all companies (for home page carousel).
    If company is set: returns signals for that company (for Earnings page).
    Ordered by score DESC.

    NOTE: This queries the local SQLite DB (earningscall_intelligence.db),
    not the PostgreSQL database — forward_signals live in the local intelligence DB.
    """
    import sqlite3
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    db_path = repo_root / "earningscall_intelligence.db"
    if not db_path.exists():
        return []

    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            # Check if table exists
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='forward_signals'"
            ).fetchall()]
            if "forward_signals" not in tables:
                return []

            clauses = ["score >= ?"]
            params: list = [min_score]
            if company:
                clauses.append("LOWER(company) = LOWER(?)")
                params.append(company.strip())
            if year:
                clauses.append("year = ?")
                params.append(int(year))
            if quarter:
                clauses.append("UPPER(quarter) = UPPER(?)")
                params.append(quarter.strip())

            where = " AND ".join(clauses)
            query = f"SELECT * FROM forward_signals WHERE {where} ORDER BY score DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


# TODO: get_forward_signals could also power:
# - Automated quarterly briefings across all companies
# - Insight generation for the Editorial page
# - Overview forward intelligence cards
# - Genie scenario planning with cross-company forward signals


def get_schema_as_string():
    """
    Get the database schema as a formatted string
    
    Returns:
        str: Formatted database schema string
    """
    try:
        # Get list of all tables
        query = """
        SELECT 
            table_name 
        FROM 
            information_schema.tables 
        WHERE 
            table_schema = 'public'
        ORDER BY 
            table_name
        """
        tables = execute_query(query)
        
        if not tables:
            return "No tables found in the database."
        
        schema_text = []
        
        for table in tables:
            table_name = table[0]
            schema_text.append(f"Table: {table_name}")
            
            # Get columns for this table
            columns_query = """
            SELECT 
                column_name, 
                data_type, 
                is_nullable 
            FROM 
                information_schema.columns 
            WHERE 
                table_name = %s 
            ORDER BY 
                ordinal_position
            """
            columns = execute_query(columns_query, (table_name,))
            
            for column in columns:
                col_name = column[0]
                col_type = column[1]
                col_nullable = "NULL" if column[2] == "YES" else "NOT NULL"
                schema_text.append(f"  - {col_name} ({col_type}) {col_nullable}")
            
            schema_text.append("")  # Add empty line between tables
        
        return "\n".join(schema_text)
    except Exception as e:
        logger.error(f"Error getting schema as string: {str(e)}")
        return f"Error getting schema: {str(e)}"
