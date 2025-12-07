import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pyhive import hive
from contextlib import contextmanager
import uvicorn


app = FastAPI()


class QueryRequest(BaseModel):
    statement: str


def get_hive_config():
    """Get Hive connection configuration from environment variables."""
    return {
        "host": os.getenv("HIVE_HOST", "localhost"),
        "port": int(os.getenv("HIVE_PORT", "10000")),
        "username": os.getenv("HIVE_USERNAME", "stackable"),
    }


@contextmanager
def get_hive_connection():
    """Context manager for Hive connection."""
    config = get_hive_config()
    conn = None
    try:
        conn = hive.Connection(
            host=config["host"],
            port=config["port"],
            username=config["username"],
        )
        yield conn
    finally:
        if conn:
            conn.close()


@app.post("/")
async def execute_query(request: QueryRequest):
    """Execute a Hive query and return results."""
    try:
        with get_hive_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(request.statement)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Convert rows to list of dicts
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            
            return {
                "status": "success",
                "columns": columns,
                "data": results,
                "row_count": len(results)
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


def main():
    """Run the FastAPI server."""
    uvicorn.run(app, host="0.0.0.0", port=10001)


if __name__ == "__main__":
    main()
