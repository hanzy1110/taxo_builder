import sqlite3
import pathlib
import pandas as pd

BASE_PATH = pathlib.Path(__file__).parent
DUMP = BASE_PATH / "DUMP"

DATOS = BASE_PATH / "datos"
RESULTADOS = BASE_PATH / "report"
DB = BASE_PATH / "DB"
DATABASE_FILE = DB / "TAXO2.db"

def get_conn(database_file = DATABASE_FILE, enforce_pk = True):
    conn = sqlite3.connect(database_file, isolation_level=None, timeout=100)
    conn.row_factory = sqlite3.Row
    if enforce_pk:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("pragma temp_store = memory;")
        conn.execute("pragma mmap_size = 30000000000;")
    return conn

def dump_database(conn):
    # Get a cursor object
    with conn:
        cursor = conn.cursor()

        # Get a list of all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        # Drop each table
        #
        with pd.ExcelWriter(DUMP/'TAXONOMIA.xlsx', engine='xlsxwriter') as writer: 
            for table in tables:
                table_name = table[0]
                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                if not df.empty:
                    df.to_excel(writer, sheet_name=table_name, index=False)

    print(f"RESUMEN GUARDADO ")


if __name__ == "__main__":
    dump_database(get_conn())
