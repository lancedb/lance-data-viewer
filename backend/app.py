#!/usr/bin/env python3

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import json

import lancedb
import pyarrow as pa
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Lance Data Viewer",
    description="Read-only web viewer for Lance datasets",
    version="0.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

DATA_PATH = Path(os.getenv("DATA_PATH", "/data"))
MAX_LIMIT = 200

def validate_dataset_name(name: str) -> bool:
    return (
        name.replace("_", "").replace("-", "").isalnum()
        and not name.startswith(".")
        and len(name) <= 100
    )

def get_lance_connection():
    if not DATA_PATH.exists():
        raise HTTPException(status_code=500, detail="Data path not found")
    return lancedb.connect(str(DATA_PATH))

def serialize_arrow_value(value):
    if pa.types.is_null(value.type):
        return None
    elif pa.types.is_boolean(value.type):
        return value.as_py()
    elif pa.types.is_integer(value.type) or pa.types.is_floating(value.type):
        return value.as_py()
    elif pa.types.is_string(value.type) or pa.types.is_large_string(value.type):
        return value.as_py()
    elif pa.types.is_timestamp(value.type):
        return value.as_py().isoformat() if value.as_py() else None
    elif pa.types.is_list(value.type) and pa.types.is_floating(value.value_type):
        vec = value.as_py()
        if vec is None:
            return None
        return {
            "type": "vector",
            "dim": len(vec),
            "norm": float(sum(x*x for x in vec) ** 0.5) if vec else 0.0,
            "min": float(min(vec)) if vec else 0.0,
            "max": float(max(vec)) if vec else 0.0,
            "preview": vec[:64] if len(vec) > 64 else vec
        }
    else:
        return str(value.as_py())

@app.get("/healthz")
async def health_check():
    return {"ok": True, "version": "0.1.0"}

@app.get("/datasets")
async def list_datasets():
    try:
        db = get_lance_connection()
        table_names = db.table_names()
        valid_tables = [name for name in table_names if validate_dataset_name(name)]
        return {"datasets": valid_tables}
    except Exception as e:
        logger.error(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail="Failed to list datasets")

@app.get("/datasets/{dataset_name}/schema")
async def get_dataset_schema(dataset_name: str):
    if not validate_dataset_name(dataset_name):
        raise HTTPException(status_code=400, detail="Invalid dataset name")

    try:
        db = get_lance_connection()
        table = db.open_table(dataset_name)
        schema = table.schema

        schema_dict = {
            "fields": [],
            "metadata": schema.metadata or {}
        }

        for field in schema:
            field_info = {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable
            }

            if pa.types.is_list(field.type) and pa.types.is_floating(field.type.value_type):
                field_info["vector_dim"] = None

            schema_dict["fields"].append(field_info)

        return schema_dict

    except Exception as e:
        logger.error(f"Error getting schema for {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get dataset schema")

@app.get("/datasets/{dataset_name}/columns")
async def get_dataset_columns(dataset_name: str):
    if not validate_dataset_name(dataset_name):
        raise HTTPException(status_code=400, detail="Invalid dataset name")

    try:
        db = get_lance_connection()
        table = db.open_table(dataset_name)
        schema = table.schema

        columns = []
        for field in schema:
            col_info = {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable
            }

            if pa.types.is_list(field.type) and pa.types.is_floating(field.type.value_type):
                col_info["is_vector"] = True
                col_info["dim"] = None
            else:
                col_info["is_vector"] = False

            columns.append(col_info)

        return {"columns": columns}

    except Exception as e:
        logger.error(f"Error getting columns for {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get dataset columns")

@app.get("/datasets/{dataset_name}/rows")
async def get_dataset_rows(
    dataset_name: str,
    limit: int = Query(default=50, le=MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
    columns: Optional[str] = Query(default=None)
):
    if not validate_dataset_name(dataset_name):
        raise HTTPException(status_code=400, detail="Invalid dataset name")

    try:
        db = get_lance_connection()
        table = db.open_table(dataset_name)

        column_list = None
        if columns:
            column_list = [col.strip() for col in columns.split(",") if col.strip()]
            schema_columns = [field.name for field in table.schema]
            invalid_columns = [col for col in column_list if col not in schema_columns]
            if invalid_columns:
                raise HTTPException(status_code=400, detail=f"Invalid columns: {invalid_columns}")

        # Get full table as Arrow Table
        full_table = table.to_arrow()
        total_count = len(full_table)

        # Apply column selection if specified
        if column_list:
            full_table = full_table.select(column_list)

        # Apply pagination manually
        start_idx = offset
        end_idx = min(offset + limit, total_count)
        result = full_table.slice(start_idx, end_idx - start_idx)

        rows = []
        for i in range(result.num_rows):
            row = {}
            for j, column_name in enumerate(result.column_names):
                value = result.column(j)[i]
                row[column_name] = serialize_arrow_value(value)
            rows.append(row)

        return {
            "rows": rows,
            "total": total_count,
            "limit": limit,
            "offset": offset
        }

    except Exception as e:
        logger.error(f"Error getting rows for {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get dataset rows")

@app.get("/datasets/{dataset_name}/vector/preview")
async def get_vector_preview(
    dataset_name: str,
    column: str,
    limit: int = Query(default=100, le=MAX_LIMIT)
):
    if not validate_dataset_name(dataset_name):
        raise HTTPException(status_code=400, detail="Invalid dataset name")

    try:
        db = get_lance_connection()
        table = db.open_table(dataset_name)

        if column not in [field.name for field in table.schema]:
            raise HTTPException(status_code=400, detail=f"Column '{column}' not found")

        field = next(field for field in table.schema if field.name == column)
        if not (pa.types.is_list(field.type) and pa.types.is_floating(field.type.value_type)):
            raise HTTPException(status_code=400, detail=f"Column '{column}' is not a vector column")

        result = table.to_arrow().select([column]).slice(0, limit)
        vectors = result.column(0).to_pylist()

        valid_vectors = [v for v in vectors if v is not None]
        if not valid_vectors:
            return {"stats": None, "preview": []}

        all_values = [val for vec in valid_vectors for val in vec]

        stats = {
            "count": len(valid_vectors),
            "dim": len(valid_vectors[0]) if valid_vectors else 0,
            "min": min(all_values) if all_values else 0,
            "max": max(all_values) if all_values else 0,
            "mean": sum(all_values) / len(all_values) if all_values else 0
        }

        preview = []
        for vec in valid_vectors[:20]:
            if vec:
                preview.append({
                    "norm": float(sum(x*x for x in vec) ** 0.5),
                    "sample": vec[:32]
                })

        return {"stats": stats, "preview": preview}

    except Exception as e:
        logger.error(f"Error getting vector preview for {dataset_name}.{column}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get vector preview")

app.mount("/", StaticFiles(directory="/web", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)