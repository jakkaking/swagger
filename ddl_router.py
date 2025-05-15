# ddl_router.py
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from database import connect_to_greenplum, table_exists, get_table_columns

router = APIRouter(prefix="/ddl", tags=["DDL"])

class DDLRequest(BaseModel):
    schema: str
    table_name: str

def map_to_teradata(col):
    name, data_type, char_len, prec, scale, nullable = col
    # (same mapping logic you had)
    if data_type in ('character varying', 'text'):
        td = f'VARCHAR({char_len or 255})'
    elif data_type == 'character':
        td = f'CHAR({char_len or 1})'
    elif data_type == 'integer': td = 'INTEGER'
    elif data_type == 'bigint': td = 'BIGINT'
    elif data_type == 'smallint': td = 'SMALLINT'
    elif data_type == 'numeric': td = f'DECIMAL({prec},{scale})' if prec and scale else 'DECIMAL(18,2)'
    elif data_type.startswith('timestamp'): td = 'TIMESTAMP(6)'
    elif data_type == 'boolean': td = 'BYTEINT'
    elif data_type == 'date': td = 'DATE'
    else: td = data_type.upper()

    td += ' CHARACTER SET LATIN NOT CASESPECIFIC'
    if nullable == 'NO': td += ' NOT NULL'
    return f'    {name.upper()} {td}'

def generate_teradata_ddl(schema, table_name, columns):
    lines = [
        f'CREATE MULTISET TABLE {schema.upper()}_T.{table_name.upper()} ,FALLBACK ,',
        '    NO BEFORE JOURNAL,',
        '    NO AFTER JOURNAL,',
        '    CHECKSUM = DEFAULT,',
        '    DEFAULT MERGEBLOCKRATIO,',
        '    MAP = TD_MAP1',
        '    ('
    ]
    lines.append(',\n'.join(map_to_teradata(c) for c in columns))
    lines.append('    )')
    lines.append('PRIMARY INDEX NUPI_PPID_SUPPLR_CAB_DTL ( PPID ,UNIQUE_CMPNT_NM );')
    return '\n'.join(lines)

@router.post("/download", summary="Generate Teradata DDL and return .sql file")
def download_ddl(req: DDLRequest):
    conn = None
    try:
        conn = connect_to_greenplum()
        if not table_exists(conn, req.schema, req.table_name):
            raise HTTPException(404, f"Table {req.schema}.{req.table_name} not found.")
        cols = get_table_columns(conn, req.schema, req.table_name)
        ddl  = generate_teradata_ddl(req.schema, req.table_name, cols)
        fname = f"{req.schema}.{req.table_name}.sql"
        with open(fname, 'w') as f:
            f.write(ddl)
        return FileResponse(path=fname, media_type='application/sql', filename=fname)
    finally:
        if conn:
            conn.close()
