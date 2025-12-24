"""
SQL Query endpoint for internal database access
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Any, List, Dict

from core.database import get_db

router = APIRouter()

class QueryRequest(BaseModel):
    sql: str
    limit: int = 100

class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    row_count: int

@router.post("/sql", response_model=QueryResponse)
async def execute_query(request: QueryRequest, db: AsyncSession = Depends(get_db)):
    """
    Execute a read-only SQL query on the database.
    Limited to SELECT statements for safety.
    """
    sql = request.sql.strip()
    
    # Security: Only allow SELECT statements
    if not sql.upper().startswith("SELECT"):
        raise HTTPException(
            status_code=400, 
            detail="Only SELECT statements are allowed"
        )
    
    # Prevent dangerous operations
    dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "CREATE", "GRANT"]
    sql_upper = sql.upper()
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            raise HTTPException(
                status_code=400,
                detail=f"Statement contains forbidden keyword: {keyword}"
            )
    
    # Add LIMIT if not present
    if "LIMIT" not in sql_upper:
        sql = f"{sql} LIMIT {request.limit}"
    
    try:
        result = await db.execute(text(sql))
        rows = result.fetchall()
        columns = list(result.keys()) if rows else []
        
        # Convert rows to list of lists (for JSON serialization)
        rows_list = [[str(val) if val is not None else None for val in row] for row in rows]
        
        return {
            "columns": columns,
            "rows": rows_list,
            "row_count": len(rows_list)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")


@router.get("/tables")
async def list_tables(db: AsyncSession = Depends(get_db)):
    """List all tables in the database"""
    sql = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
    """
    result = await db.execute(text(sql))
    rows = result.fetchall()
    
    tables = {}
    for schema, table in rows:
        if schema not in tables:
            tables[schema] = []
        tables[schema].append(table)
    
    return {"tables": tables}


@router.get("/tables/{schema}/{table}/columns")
async def get_table_columns(schema: str, table: str, db: AsyncSession = Depends(get_db)):
    """Get columns for a specific table"""
    sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
    """
    result = await db.execute(text(sql), {"schema": schema, "table": table})
    rows = result.fetchall()
    
    columns = [
        {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
        for row in rows
    ]
    
    return {"schema": schema, "table": table, "columns": columns}


@router.get("/tables/{schema}/{table}/preview")
async def preview_table(schema: str, table: str, limit: int = 10, db: AsyncSession = Depends(get_db)):
    """Preview data from a table"""
    # Sanitize schema and table names
    if not schema.isidentifier() or not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid schema or table name")
    
    sql = f'SELECT * FROM "{schema}"."{table}" LIMIT {min(limit, 100)}'
    
    try:
        result = await db.execute(text(sql))
        rows = result.fetchall()
        columns = list(result.keys()) if rows else []
        rows_list = [[str(val) if val is not None else None for val in row] for row in rows]
        
        return {
            "schema": schema,
            "table": table,
            "columns": columns,
            "rows": rows_list,
            "row_count": len(rows_list)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")

