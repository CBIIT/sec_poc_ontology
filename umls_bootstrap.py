import sys

import pandas as pd
import sqlite3
import pathlib
import psycopg2
import psycopg2.extras
import sqlalchemy
import argparse
import datetime

parser = argparse.ArgumentParser(description='Selectively bootstrap a sqlite or Postgresql UMLS database.')

parser.add_argument('--dbname', action='store', type=str, required=False)
parser.add_argument('--host', action='store', type=str, required=False)
parser.add_argument('--user', action='store', type=str, required=False)
parser.add_argument('--password', action='store', type=str, required=False)
parser.add_argument('--port', action='store', type=str, required=False)
parser.add_argument('--schema', action='store', type=str, required=False)
parser.add_argument('--ontologies', action='store', type=str, required=True)
parser.add_argument('--dbfilename', action='store', type=str, required=False )
parser.add_argument('--umls_data_dir', action='store', type=str, required=True)

#ontologies = ['NCI', 'SNOMEDCT_US', 'CPT', 'ICD10PCS', 'ICD10CM', 'RXNORM', 'ICD9CM']
#ontologies = [ 'ICD10CM']

args = parser.parse_args()
if args.dbfilename is None:
    connection_string = f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.dbname}'
else:
    connection_string = args.dbfilename

ontologies = args.ontologies.split(',')

def fix_sql(sql):
    if args.dbfilename is None:
        s = sql.replace('?', '%s')
    else:
        s = sql
    return s

if args.dbfilename is None:
    print('UMLS Bootstrap -- using Postgresql')
    if args.schema is not None:
        sae_connection = sqlalchemy.create_engine(connection_string,
                                                 connect_args={'options': '-csearch_path={}'.format(args.schema)}, future=True)
    else:
        sae_connection = sqlalchemy.create_engine(connection_string)
    sa_connection = sae_connection.connect()
    db_connection = psycopg2.connect(database=args.dbname, user=args.user, host=args.host, port=args.port,
                           password=args.password)
else:
    db_connection = sqlite3.connect(connection_string)
    sa_connection = db_connection

cur = db_connection.cursor()

def import_table_pandas(table_name, columns):
    print('Importing via Pandas', table_name)
    #db.execute('delete from ' + table_name)
    if args.schema is not None:
        cur.execute('drop table if exists '+args.schema+'.'+table_name)
    else:
        cur.execute('drop table if exists ' + table_name)
    db_connection.commit()
    df = pd.read_csv(pathlib.Path(args.umls_data_dir).joinpath(table_name+'.RRF'), delimiter='|', header=None)
    print(len(df.columns))
    df = df.drop(df.columns[len(df.columns) - 1], axis=1)
    print(df)
    df.columns = columns
    df.columns = [x.lower() for x in df.columns]
    if args.schema is not None:
        df.to_sql(table_name.lower(),sa_connection , schema=args.schema, if_exists='append', index=False)
    else:
        df.to_sql(table_name.lower(),sa_connection , if_exists='append', index=False)
    sa_connection.commit()
    db_connection.commit()

def import_table_pandas_chunks(table_name):

    print('Importing via Pandas in chunks ', table_name)
    if args.schema is not None:
        cur.execute('drop table if exists ' + args.schema + '.' + table_name)
    else:
        cur.execute('drop table if exists ' + table_name)

    # now recreate the table

    if args.schema is not None:
        fmt_sql = "select fmt from "+args.schema+".mrfiles where fil = ?"
    else:
        fmt_sql = "select fmt from mrfiles where fil = ?"
    fmt_sql = fix_sql(fmt_sql)

    rc = cur.execute(fmt_sql, [table_name + '.RRF'.upper()])
    rs= cur.fetchone()[0]
    print(rs)

    if args.schema is not None:
        sql_create = 'create table ' + args.schema+'.'+table_name.lower() + '('
    else:
        sql_create = 'create table ' + table_name.lower() +  '('

    rs = rs + ',foobar'
    column_names = rs.split(',')
    sql_cols = []
    for c in column_names:
        if c != 'foobar':
            if args.schema is not None:
                col_sql = 'select col, dty from '+args.schema+'.mrcols where fil = ? and col = ?'
            else:
                col_sql = 'select col, dty from mrcols where fil = ? and col = ?'
            col_sql = fix_sql(col_sql)
            rc = cur.execute(col_sql,
                                [table_name + '.RRF'.upper(),c])
            rs_col = cur.fetchone()[1]
            print(rs_col)
            sql_cols.append( c.lower() + " " + rs_col )
    print(sql_cols)
    sql_create = sql_create + ",".join(sql_cols) + ")"
    print(sql_create)
    cur.execute(sql_create)
    db_connection.commit()
    tab_dtypes = {}
    for c in column_names:
        tab_dtypes[c] = 'object'
    #db.execute('delete from ' + table_name)
    df_chunk = pd.read_csv(pathlib.Path(args.umls_data_dir).joinpath(table_name + '.RRF'), delimiter='|', header=None, chunksize=200000,
                           names = column_names, dtype=tab_dtypes)

    chunk_num = 1
    for df in df_chunk:
        print("chunk ", chunk_num, 'of', table_name , 'has', len(df) , 'rows')
        df = df.loc[df['SAB'].isin(ontologies)]
        print("after purge has", len(df), "rows")
        df = df.drop(df.columns[len(df.columns) - 1], axis=1)
        df.columns = [x.lower() for x in df.columns]
        if args.schema is not None:
            df.to_sql(table_name.lower(), sa_connection, schema=args.schema, if_exists='append', index=False)
        else:
            df.to_sql(table_name.lower(), sa_connection, if_exists='append', index=False)
        chunk_num += 1
        sa_connection.commit()

    db_connection.commit()


#
# Bootstrap with MRCOLS and MRFILES tables so the DDL can be generated for all other tables with proper datatypes
#
import_table_pandas('MRCOLS', columns = [ 'COL','DES','REF' ,'MIN','AV','MAX','FIL','DTY'])
import_table_pandas('MRFILES', columns = ['FIL','DES','FMT','CLS','RWS','BTS'])
import_table_pandas('MRDOC', columns = ['DOCKEY','VALUE','TYPE','EXPL'])

# Now bring in other needed UMLS tables

import_table_pandas_chunks('MRCONSO')
import_table_pandas_chunks('MRREL')
import_table_pandas_chunks('MRDEF')
import_table_pandas_chunks('MRHIER')

print(datetime.datetime.now(), "Creating indexes")
if args.schema is not None:
    cur.execute("create index mrconso_cui on "+args.schema+".mrconso(cui)")
else:
    cur.execute("create index mrconso_cui on mrconso(cui)")
db_connection.commit()

if args.schema is not None:
    cur.execute("create index rel_cui1 on "+args.schema+".mrrel(cui1)")
else:
    cur.execute("create index rel_cui1 on mrrel(cui1)")
db_connection.commit()

if args.schema is not None:
    cur.execute("create index rel_cui2 on "+args.schema+".mrrel(cui2)")
else:
    cur.execute("create index rel_cui2 on mrrel(cui2)")
db_connection.commit()

if args.schema is not None:
    cur.execute("create index hier_sab on "+args.schema+".mrhier(sab)")
else:
    cur.execute("create index hier_sab on mrhier(sab)")
db_connection.commit()

if args.schema is not None:
    cur.execute("create index hier_aui on "+args.schema+".mrhier(aui)")
else:
    cur.execute("create index hier_aui on mrhier(aui)")
db_connection.commit()

if args.schema is not None:
    cur.execute("create index hier_paui on "+args.schema+".mrhier(paui)")
else:
    cur.execute("create index hier_paui on mrhier(paui)")
db_connection.commit()

if args.schema is not None:
    cur.execute("create index conso_aui on "+args.schema+".mrconso(aui)")
else:
    cur.execute("create index conso_aui on mrconso(aui)")
db_connection.commit()

#create index hier_aui on mrhier(aui);
#create index heir_paui on mrhier(paui);
#create index conso_aui on mrconso(aui);
# create index mrconso_cui on mrconso(cui);
# create index mrconso_sab on mrconso(sab, code, tty);

#create index rel_cui1 on mrrel(cui1);
#create index rel_cui2 on mrrel(cui2);
#create index rel_sab on mrrel(sab);

db_connection.close()