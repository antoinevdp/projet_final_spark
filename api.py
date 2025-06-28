from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db, get_table_names, get_table, engine
import pandas as pd
from typing import Optional, Dict, Any, List
import json

app = FastAPI(title="MariaDB Data API", description="API to display data from MariaDB tables")


@app.get("/")
def root():
    return {"message": "MariaDB Data API",
            "endpoints": ["/tables", "/tables/{table_name}", "/tables/{table_name}/data"]}


@app.get("/tables")
def list_tables():
    """Get list of all tables in the database"""
    try:
        tables = get_table_names()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}")
def get_table_info(table_name: str):
    """Get information about a specific table (columns, types, etc.)"""
    try:
        table = get_table(table_name)
        if table is None:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        columns_info = []
        for column in table.columns:
            columns_info.append({
                "name": column.name,
                "type": str(column.type),
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "autoincrement": column.autoincrement
            })

        return {
            "table_name": table_name,
            "columns": columns_info,
            "total_columns": len(columns_info)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/data")
def get_table_data(
        table_name: str,
        limit: Optional[int] = Query(100, ge=1, le=1000, description="Number of records to return"),
        offset: Optional[int] = Query(0, ge=0, description="Number of records to skip"),
        db: Session = Depends(get_db)
):
    """Get data from a specific table with pagination"""
    try:
        # Check if table exists
        table = get_table(table_name)
        if table is None:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        # Build query with pagination
        query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"

        # Execute query using pandas for easier JSON serialization
        df = pd.read_sql(query, engine)

        # Convert DataFrame to records
        records = df.to_dict('records')

        # Get total count
        count_query = f"SELECT COUNT(*) as total FROM {table_name}"
        total_count = pd.read_sql(count_query, engine)['total'].iloc[0]

        return {
            "table_name": table_name,
            "data": records,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total_records": int(total_count),
                "returned_records": len(records)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/search")
def search_table_data(
        table_name: str,
        column: str,
        value: str,
        limit: Optional[int] = Query(100, ge=1, le=1000),
        db: Session = Depends(get_db)
):
    """Search for records in a table based on column value"""
    try:
        # Check if table exists
        table = get_table(table_name)
        if table is None:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        # Check if column exists
        if column not in [col.name for col in table.columns]:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found in table '{table_name}'")

        # Build search query
        query = f"SELECT * FROM {table_name} WHERE {column} LIKE '%{value}%' LIMIT {limit}"

        # Execute query
        df = pd.read_sql(query, engine)
        records = df.to_dict('records')

        return {
            "table_name": table_name,
            "search_criteria": {"column": column, "value": value},
            "data": records,
            "found_records": len(records)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)