#!/usr/bin/env python3
"""Load a CSV into Postgres using psycopg2. Creates table if not exists with best-effort types."""
import argparse, csv, psycopg2, pandas as pd, io

def to_sql_type(series):
    if pd.api.types.is_integer_dtype(series):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION'
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP'
    return 'TEXT'

def create_table_from_df(df, table, cur):
    cols = []
    for col in df.columns:
        sql_type = to_sql_type(df[col])
        cols.append(f'"{col}" {sql_type}')
    ddl = f'CREATE TABLE IF NOT EXISTS {table} (' + ', '.join(cols) + ');'
    cur.execute(ddl)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True)
    parser.add_argument('--table', default='tickets_raw')
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--db-user', required=True)
    parser.add_argument('--db-pass', required=True)
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', default='5432')
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    # basic dtype attempts
    for col in df.columns:
        # try parse date-like columns
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    conn = psycopg2.connect(dbname=args.db_name, user=args.db_user, password=args.db_pass, host=args.db_host, port=args.db_port)
    cur = conn.cursor()
    create_table_from_df(df, args.table, cur)
    conn.commit()

    # Use COPY FROM to load fast
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=True)
    buffer.seek(0)
    cur.copy_expert(f'COPY {args.table} FROM STDIN WITH CSV HEADER', buffer)
    conn.commit()
    cur.close()
    conn.close()
    print('Loaded CSV into', args.table)

if __name__ == '__main__':
    main()