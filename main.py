# main.py
from fastapi import FastAPI
from ddl_router import router as ddl_router
from mapping_router import router as mapping_router

app = FastAPI(
    title="Greenplum→Teradata Tools",
    description="Generate Teradata DDL and Excel mappings from Greenplum metadata",
    version="1.0"
)

app.include_router(ddl_router)
app.include_router(mapping_router)
