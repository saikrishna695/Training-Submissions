""" This is a no schema based ETL script to load JSON data into postgree SQL
    To run this file make sure you have already installed the 'psycopg' package prior to execution"""

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Iterable, Dict, Any, List, Tuple

import psycopg
from psycopg.rows import dict_row
from psycopg import sql


def parse_args():
    p = argparse.ArgumentParser(description="Load JSON into PostgreSQL.")
    p.add_argument("--input", required=True, help="Path to JSON file (array or NDJSON).")
    p.add_argument("--table", required=True, help="Destination table name.")
    p.add_argument("--mode", choices=["jsonb", "typed"], default="jsonb",
                   help="jsonb: store each record in a JSONB column; typed: map columns.")
    p.add_argument("--columns", nargs="*", default=[],
                   help="Typed columns as name:pgtype (e.g., id:int name:text created_at:timestamptz).")
    p.add_argument("--upsert-key", default=None,
                   help="Column name to use for UPSERT (typed mode only).")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Rows per batch insert.")
    return p.parse_args()


def load_json_records(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        head = f.read(1)
        if not head:
            return []
        f.seek(0)
        if head == "[":
            data = json.load(f)
            if isinstance(data, dict):
                data = [data]
            return data
        else:
            records = []
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
            return records


def get_conn():
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        row_factory=dict_row,
        autocommit=False,
    )


def ensure_jsonb_table(cur, table: str):
    cur.execute(
        sql.SQL().format(sql.Identifier(table))
    )


def parse_typed_columns(specs: List[str]) -> List[Tuple[str, str]]:
    cols = []
    for spec in specs:
        if ":" not in spec:
            raise ValueError(f"Column spec '{spec}' missing ':' (use name:pgtype).")
        name, pgtype = spec.split(":", 1)
        name = name.strip()
        pgtype = pgtype.strip()
        if not name or not pgtype:
            raise ValueError(f"Bad column spec '{spec}'.")
        cols.append((name, pgtype))
    return cols


def ensure_typed_table(cur, table: str, cols: List[Tuple[str, str]], upsert_key: str | None):
    col_defs = [f'{name} {pgtype}' for name, pgtype in cols]
    if "ingested_at" not in [c[0] for c in cols]:
        col_defs.append("ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
    ddl = f"CREATE TABLE IF NOT EXISTS {psycopg.sql.Identifier(table).as_string(cur)} ({', '.join(col_defs)})"
    cur.execute(ddl)
    if upsert_key:
        cur.execute(sql.SQL(), (f"{table}_{upsert_key}_key", f"{table}_{upsert_key}_key", table, upsert_key))


def chunked(iterable, size):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def rows_for_typed(records: List[Dict[str, Any]], cols: List[Tuple[str, str]]) -> List[Tuple[Any, ...]]:
    out = []
    for rec in records:
        row = []
        for name, _pgtype in cols:
            if name == "ingested_at":
                row.append(None)
                continue
            val = rec.get(name, None)
            row.append(val)
        out.append(tuple(row))
    return out


def insert_jsonb(cur, table: str, records: List[Dict[str, Any]]):
    query = sql.SQL("INSERT INTO {} (payload) VALUES (to_jsonb(%s::json))").format(sql.Identifier(table))
    params = [json.dumps(r, ensure_ascii=False) for r in records]
    cur.executemany(query.as_string(cur), [(p,) for p in params])


def insert_typed(cur, table: str, cols: List[Tuple[str, str]],
                 records: List[Dict[str, Any]], upsert_key: str | None):
    col_names = [c[0] for c in cols]
    placeholders = ", ".join(["%s"] * len(cols))
    col_idents = sql.SQL(", ").join([sql.Identifier(c) for c in col_names])
    rows = rows_for_typed(records, cols)

    if upsert_key and upsert_key in col_names:
        excluded = sql.SQL(", ").join([
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in col_names if c != upsert_key
        ])
        q = sql.SQL().format(
            sql.Identifier(table),
            col_idents,
            sql.SQL(placeholders),
            sql.Identifier(upsert_key),
            excluded
        )
    else:
        q = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            col_idents,
            sql.SQL(placeholders)
        )

    cur.executemany(q.as_string(cur), rows)


def main():
    args = parse_args()
    records = load_json_records(args.input)
    if not records:
        print("No records found in input.")
        return

    now = datetime.now(timezone.utc).isoformat()
    for r in records:
        r.setdefault("ingested_at", now)

    with get_conn() as conn:
        with conn.cursor() as cur:
            if args.mode == "jsonb":
                ensure_jsonb_table(cur, args.table)
                for batch in chunked(records, args.batch_size):
                    insert_jsonb(cur, args.table, batch)
                    conn.commit()
                print(f"Inserted {len(records)} rows into {args.table} (jsonb mode).")
            else:
                cols = parse_typed_columns(args.columns)
                if not cols:
                    raise SystemExit("Typed mode requires --columns like name:text id:int ...")
                ensure_typed_table(cur, args.table, cols, args.upsert_key)
                for batch in chunked(records, args.batch_size):
                    insert_typed(cur, args.table, cols, batch, args.upsert_key)
                    conn.commit()
                print(f"Upserted/Inserted {len(records)} rows into {args.table} (typed mode).")


if __name__ == "__main__":
    main()
