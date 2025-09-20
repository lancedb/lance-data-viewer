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
    try:
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
            try:
                vec = value.as_py()
                if vec is None:
                    return None

                # Validate vector data
                if not isinstance(vec, (list, tuple)) or len(vec) == 0:
                    return {"type": "vector", "error": "Invalid vector data"}

                # Check for valid numeric values
                valid_values = []
                for v in vec:
                    if v is not None and isinstance(v, (int, float)) and not (isinstance(v, float) and (v != v or v == float('inf') or v == float('-inf'))):
                        valid_values.append(float(v))
                    else:
                        valid_values.append(0.0)  # Replace invalid values with 0

                if not valid_values:
                    return {"type": "vector", "error": "No valid numeric values in vector"}

                return {
                    "type": "vector",
                    "dim": len(valid_values),
                    "norm": float(sum(x*x for x in valid_values) ** 0.5) if valid_values else 0.0,
                    "min": float(min(valid_values)) if valid_values else 0.0,
                    "max": float(max(valid_values)) if valid_values else 0.0,
                    "preview": valid_values[:64] if len(valid_values) > 64 else valid_values
                }
            except Exception as vec_error:
                logger.warning(f"Error processing vector data: {vec_error}")
                return {"type": "vector", "error": f"Vector processing failed: {str(vec_error)}"}
        else:
            return str(value.as_py())
    except Exception as e:
        logger.warning(f"Error serializing value: {e}")
        return {"error": f"Serialization failed: {str(e)}"}

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

        # Try different approaches to read the data safely
        result_table = None
        total_count = 0

        try:
            # First attempt: try to get schema-only info for severely corrupted datasets
            try:
                schema = table.schema
                schema_columns = [field.name for field in schema]

                # For severely corrupted datasets, just return schema info with no data
                if "panic" in str(Exception) or "corrupted" in dataset_name.lower():
                    raise Exception("Dataset appears corrupted, returning schema-only")

                # Try progressively more cautious reading approaches
                full_table = None

                # Method 1: Try pandas conversion
                try:
                    if hasattr(table, 'head'):
                        df = table.head(min(limit + offset, 100))  # Limit to avoid memory issues
                        # Convert problematic columns carefully
                        for col in df.columns:
                            if df[col].dtype == object:
                                # Check if this is a vector column that might be problematic
                                sample_val = df[col].iloc[0] if len(df) > 0 else None
                                if isinstance(sample_val, (list, tuple)) and len(sample_val) > 100:
                                    # Very large vectors - truncate for safety
                                    df[col] = df[col].apply(lambda x: x[:64] if isinstance(x, (list, tuple)) else x)
                        full_table = pa.Table.from_pandas(df)
                    else:
                        raise Exception("No head method available")

                except Exception as pandas_err:
                    logger.warning(f"Pandas approach failed for {dataset_name}: {pandas_err}")

                    # Method 2: Try direct Arrow access with small chunks
                    try:
                        if hasattr(table, 'to_batches'):
                            # Try to read just one small batch
                            batch_iter = table.to_batches(max_chunksize=10)
                            first_batch = next(batch_iter)
                            full_table = pa.Table.from_batches([first_batch])
                        else:
                            # Last resort: try to_arrow() but be prepared for failure
                            full_table = table.to_arrow()

                    except Exception as arrow_err:
                        logger.error(f"Direct Arrow access failed for {dataset_name}: {arrow_err}")

                        # Method 3: Return error info but keep the API working
                        error_schema = pa.schema([
                            pa.field("error", pa.string()),
                            pa.field("dataset", pa.string()),
                            pa.field("issue", pa.string())
                        ])
                        error_data = [
                            ["Dataset corrupted or unreadable"],
                            [dataset_name],
                            [f"Lance backend error: {str(arrow_err)[:100]}"]
                        ]
                        full_table = pa.Table.from_arrays(error_data, schema=error_schema)
                        total_count = 1

                        # Apply pagination for error case
                        if offset == 0:
                            result_table = full_table.slice(0, min(limit, 1))
                        else:
                            result_table = full_table.slice(0, 0)

                if full_table is not None and "error" not in [field.name for field in full_table.schema]:
                    total_count = len(full_table)

                    # Apply column selection if specified
                    if column_list:
                        available_columns = [col for col in column_list if col in full_table.column_names]
                        if available_columns:
                            full_table = full_table.select(available_columns)

                    # Apply pagination manually
                    start_idx = offset
                    end_idx = min(offset + limit, total_count)

                    if start_idx < total_count:
                        result_table = full_table.slice(start_idx, end_idx - start_idx)
                    else:
                        # Create empty table with same schema
                        result_table = full_table.slice(0, 0)

            except Exception as schema_error:
                logger.error(f"Even schema reading failed for {dataset_name}: {schema_error}")

                # Ultimate fallback: return a helpful error message as data
                error_schema = pa.schema([
                    pa.field("status", pa.string()),
                    pa.field("message", pa.string())
                ])
                error_data = [
                    ["error"],
                    [f"Dataset {dataset_name} is corrupted and cannot be read"]
                ]
                result_table = pa.Table.from_arrays(error_data, schema=error_schema)
                total_count = 1

        except Exception as general_error:
            logger.error(f"Complete failure for {dataset_name}: {general_error}")
            raise HTTPException(status_code=500, detail=f"Dataset {dataset_name} is completely inaccessible")

        rows = []
        for i in range(result_table.num_rows):
            row = {}
            for j, column_name in enumerate(result_table.column_names):
                try:
                    value = result_table.column(j)[i]
                    row[column_name] = serialize_arrow_value(value)
                except Exception as serialize_error:
                    logger.warning(f"Failed to serialize column {column_name} at row {i}: {serialize_error}")
                    row[column_name] = {"error": "Failed to read value"}
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

# Only mount static files if the directory exists (for production)
if os.path.exists("/web"):
    app.mount("/", StaticFiles(directory="/web", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)