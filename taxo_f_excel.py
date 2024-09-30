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

def generate_update_query(table_name: str, update_dict, constraint) -> str:
    set_clause = ', '.join([f"{key} = {parse_value(value)}" for key, value in update_dict.items()])
    where_clause = f"id = {constraint}"
    sql_update_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
    return sql_update_query

def generate_search_query(table_name, data_dict):
    data_dict.pop("id", None)
    
    query = f"SELECT * FROM {table_name} WHERE "
    conditions = []
    for field, value in data_dict.items():
        if isinstance(value, str):
            conditions.append(f"{field} LIKE '{value}'")
        elif value is None:
            continue
        else:
            conditions.append(f"{field} = {value}")
    query += " AND ".join(conditions)
    
    return query

def compare_dicts(obj1:dict, obj2:dict):
    differences = []
    
    for name in obj1.keys():
        if name == "id":
            continue

        value1, valid_1 = parse_missing(obj1.get(name, None))
        value2, valid_2 = parse_missing(obj2.get(name, None))

        # Dos valores invalidos implican que estan vacios
        if valid_1 and valid_2:
            if value1 != value2:
                differences.append((name, value1, value2))
        elif valid_2:
            differences.append((name, value1, value2))
    
    return differences

def valid_diff(ds):
    fields = {d[0] for d in ds}
    return "modelo" not in fields and "accesorio" not in fields and fields

def get_correct_update(ups):
    for u in ups:
        if "capacidad" in u or "funcion" in u or "traccion" in u or "cabinas" in u or "tipo" in u:
            return u

def manage_diffs(mins: dict, table:str):

    updates = defaultdict(lambda: [])
    qs = {}

    for k, diffs in mins.items():
        for ds in diffs:
            if valid_diff(ds):
                update_dict = {v[0]: v[2] for v in ds}
                # if update_dict:
                updates[k].append(update_dict)

    for k, ups in updates.items():
        lens = [(i,len(u)) for i, u in enumerate(ups)]
        lens_sort = list(sorted(lens, key=lambda x: x[1]))
        if lens == lens_sort:
            to_query = get_correct_update(ups)
            if to_query is None:
                print("EMPTY UPDATE =>> ", ups)
                continue
        else:
            to_query = ups[lens_sort[-1][0]]

        qs[k] = generate_update_query(table, to_query, k)

    return qs

def proc_df(table: str, df: pd.DataFrame, c:sqlite3.Cursor):

    db_data = [dict(d) for d in c.execute(f"SELECT * FROM {table}")]
    mins = {}

    for d_old in db_data:
        diffs = [compare_dicts(d_old, d_new) for d_new in df.to_dict("records")]
        mins[d_old["id"]] = list(sorted(diffs, key=lambda x: len(x)))

    qs = manage_diffs(mins, table)
    
    for q in qs.values():
        res = c.execute(q)
        print("UPDATE DONE =>> ", res)

    # with open(RESULTADOS/"min_diffs.json", "w") as f:
    #     json.dump(qs, f, indent=2)

def main():
    taxo = pd.read_excel(DATOS / 'TAXONOMIA.xlsx', sheet_name=None)

    taxo.pop("IH08", None)
    taxo.pop("IE36", None)
    taxo.pop("IH08F", None)
    taxo.pop("Taxonomia", None)

    with get_conn() as conn:
        c = conn.cursor()
        for k, v in taxo.items():
            print("SHEET => ", k)
            proc_df(k, v, c)

if __name__=="__main__":
    main()
