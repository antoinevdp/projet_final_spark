from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, asc
from typing import Optional, List, Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Traffic Accident Analytics API",
    description="REST API for traffic accident data analytics using Apache Spark",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Initialize Spark Session
def get_spark_session():
    if not hasattr(get_spark_session, "spark"):
        get_spark_session.spark = (SparkSession.builder
                                   .appName("TrafficAccidentAPI")
                                   .config("spark.jars", "mysql-connector-j-9.3.0.jar")
                                   .config("spark.sql.adaptive.enabled", "true")
                                   .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                                   .enableHiveSupport()
                                   .getOrCreate())
        get_spark_session.spark.sparkContext.setLogLevel("WARN")
    return get_spark_session.spark


# Database configuration
jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME', 'accidents')}"
connection_properties = {
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "driver": "com.mysql.cj.jdbc.Driver"
}


def read_table_with_spark(table_name: str) -> "DataFrame":
    """Read table using Spark from MySQL"""
    spark = get_spark_session()
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_properties
        )
        return df
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found or connection error: {str(e)}")


def get_available_tables() -> List[str]:
    """Get list of available tables from MySQL"""
    spark = get_spark_session()
    try:
        # Get tables from information_schema
        tables_df = spark.read.jdbc(
            url=jdbc_url,
            table="(SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()) as tables",
            properties=connection_properties
        )
        return [row.table_name for row in tables_df.collect()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tables: {str(e)}")


@app.get("/")
async def root():
    return {
        "message": "Traffic Accident Analytics API powered by Apache Spark",
        "version": "1.0.0",
        "endpoints": {
            "tables": "/tables",
            "documentation": "/docs",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    try:
        spark = get_spark_session()
        spark.sql("SELECT 1").collect()
        return {"status": "healthy", "spark": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/tables")
async def list_tables():
    """List all available tables"""
    try:
        tables = get_available_tables()
        return {
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}")
async def get_table_info(table_name: str):
    """Get table schema and basic information"""
    try:
        df = read_table_with_spark(table_name)

        # Get schema information
        schema_info = []
        for field in df.schema.fields:
            schema_info.append({
                "column": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable
            })

        # Get row count
        row_count = df.count()

        return {
            "table_name": table_name,
            "row_count": row_count,
            "column_count": len(schema_info),
            "schema": schema_info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/data")
async def get_table_data(
        table_name: str,
        limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
        offset: int = Query(0, ge=0, description="Number of rows to skip"),
        sort_by: Optional[str] = Query(None, description="Column to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order")
):
    """Get table data with pagination and sorting"""
    try:
        df = read_table_with_spark(table_name)

        # Apply sorting if specified
        if sort_by:
            if sort_by not in df.columns:
                raise HTTPException(status_code=400, detail=f"Column '{sort_by}' not found")

            if sort_order == "desc":
                df = df.orderBy(desc(sort_by))
            else:
                df = df.orderBy(asc(sort_by))

        # Apply pagination
        paginated_df = df.offset(offset).limit(limit)

        # Convert to list of dictionaries
        data = []
        for row in paginated_df.collect():
            data.append(row.asDict())

        # Get total count for pagination info
        total_count = df.count()

        return {
            "table_name": table_name,
            "data": data,
            "pagination": {
                "total_rows": total_count,
                "returned_rows": len(data),
                "offset": offset,
                "limit": limit,
                "has_more": offset + limit < total_count
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/spark/info")
async def get_spark_info():
    """Get Spark session information"""
    try:
        spark = get_spark_session()
        return {
            "spark_version": spark.version,
            "app_name": spark.sparkContext.appName,
            "master": spark.sparkContext.master,
            "executor_memory": spark.conf.get("spark.executor.memory", "Not set"),
            "driver_memory": spark.conf.get("spark.driver.memory", "Not set"),
            "sql_adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled", "false")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Graceful shutdown
@app.on_event("shutdown")
async def shutdown_event():
    if hasattr(get_spark_session, "spark"):
        get_spark_session.spark.stop()