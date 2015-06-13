#!/usr/bin/env python
"""
Create a table for and load a CSV into Postgres
"""

import sys, csv, time, argparse, getpass, itertools
import psycopg2

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("file", metavar="FILE", help="the csv file")
parser.add_argument("--user", help="postgres username")
parser.add_argument("--password", help="postgres password")
parser.add_argument("--host", help="postgres host", default="localhost")
parser.add_argument("--port", help="postgres port", type=int, default=5432)
parser.add_argument("--db", help="postgres db")
parser.add_argument("--table", help="postgres table name")
parser.add_argument("--drop", help="drop table before creating", action='store_true', default=True)
parser.add_argument("--detect", help="number of rows to use for type detection", type=int, default=1000)
parser.add_argument("--key", help="column of the primary key", type=int, default=0)
args = vars(parser.parse_args())

def build_table_format(source, size, pkey):
    """
    Attempts to intellgiently detect the type of columns in our data set,
    and builds a mapping of column name too column type. This is later
    used to generate the PG table.

    This function uses a sample of our dataset to detect the type of columns.
    """
    reader = csv.reader(source)

    rows = []
    for i, row in enumerate(reader):
        rows.append(row)

        if i >= args['detect']:
            break

    type_data = {i: set() for i in range(len(rows[0]))}

    # First lets iterate over our sample set and grab the types we match
    for row in rows[1:]:
        for i, col in enumerate(row):
            if col.isdigit():
                type_data[i].add(int)
                continue

            if col.lower() in ['true', 'false']:
                type_data[i].add(bool)
                continue

            try:
                float(col)
                type_data[i].add(float)
                continue
            except: pass

            type_data[i].add(str)

    # Now, lets iterate over our columns and make a choice based on the possible
    # types inside our sample set.
    result = []
    for col, data in type_data.iteritems():
        # If we are the primary key and valid, just pick bigint
        if col == pkey and len(data) == 1 and list(data)[0] == int:
            result.append('BIGINT PRIMARY KEY')
            continue

        if str in data:
            result.append('TEXT')
        elif float in data:
            result.append('DECIMAL')
        elif int in data:
            result.append('BIGINT')
        elif bool in data:
            result.append('BOOLEAN')
        else:
            result.append('TEXT')

    return [(rows[0][i].lower().replace(' ', '_'), v) for i, v in enumerate(result)]

TABLE_CREATE_SQL = """
CREATE TABLE {} (
    {}
);
"""

def create_table(conn, name, cols, drop):
    sql = TABLE_CREATE_SQL.format(name, ',\n    '.join(map(lambda i: ' '.join(i), cols)))

    if drop:
        sql = "DROP TABLE IF EXISTS {};\n".format(name) + sql

    c = conn.cursor()
    c.execute(sql)
    conn.commit()

def load_data(db, table, source):
    source.seek(0)

    db.cursor().copy_expert("COPY {} FROM STDIN WITH CSV HEADER".format(table), source)
    db.commit()

def open_connection(user, pw, host, port, db):
    pw = pw or getpass.getpass("Postgres PW: ")
    return psycopg2.connect("user={user} host={host} port={port} dbname={db} password={pw}".format(
        user=user,
        host=host,
        port=port,
        db=db,
        pw=pw))

def main():
    start = time.time()

    source = open(args['file'], "r")

    print "Detecting table format..."
    table_fmt = build_table_format(source, args['detect'], args['key'])
    for k, v in table_fmt:
        print "  {}: {}".format(k, v)

    db = open_connection(args['user'], args['password'], args['host'], args['port'], args['db'])

    print "Creating table..."
    create_table(db, args['table'], table_fmt, args['drop'])

    print "Loading data..."
    load_data(db, args['table'], source)

    print "Finished in {}s".format(time.time() - start)

if __name__ == "__main__":
    sys.exit(main())

