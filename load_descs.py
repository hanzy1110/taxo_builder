from collections import defaultdict
import json
import sqlite3
import pathlib
import pandas as pd

BASE_PATH = pathlib.Path(__file__).parent
DATOS = BASE_PATH / "datos"
RESULTADOS = BASE_PATH / "report"
DB = BASE_PATH / "DB"
DATABASE_FILE = DB / "TAXO2.db"

def parse_value(val):
    if isinstance(val, str):
        val = val.replace("'", "")
        return f"'{val}'"
    elif val is None:
        return "NULL"
    elif str(val) == 'nan':
        return "NULL"
    return f"'{str(val)}'"

def parse_missing(val):
    if val == "N/A":
        return val, False
    elif pd.isna(val):
        return val, False

    return val, bool(val)

def get_conn(database_file = DATABASE_FILE, enforce_pk = True):
    conn = sqlite3.connect(database_file, isolation_level=None, timeout=100)
    conn.row_factory = sqlite3.Row
    if enforce_pk:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("pragma temp_store = memory;")
        conn.execute("pragma mmap_size = 30000000000;")
    return conn

def generate_insert_query(table_name: str, data_dict: dict) -> str:
    try:
        del data_dict["id"]
    except KeyError as e:
        pass

    columns = ', '.join(data_dict.keys())
    values = ', '.join([parse_value(value) for value in data_dict.values()])
    
    sql_insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    return sql_insert_query

INIT_CLASES = """
CREATE TABLE IF NOT EXISTS Clases (
    id INTEGER PRIMARY KEY,
    clase TEXT,
    denominacion_clase TEXT,
    subclase TEXT,
    denominacion_subclase TEXT
);
"""


def main():
    descs = pd.read_excel(DATOS / "clase_y_subclases.xlsx")

    descs = descs.rename(columns={
        "CLASE":"clase",
        "DENOMINACION CLASE":"denominacion_clase",
        "SUBCLASE": "subclase",
        "DENOMINACION SUBCLASE": "denominacion_subclase"
    })

    qs = []
    for d in descs.to_dict("records"):
        qs.append(generate_insert_query("Clases", d))

    with get_conn() as conn:
        c = conn.cursor()
        c.execute(INIT_CLASES)
        for q in qs:
            c.execute(q)

if __name__=="__main__":
    main()
