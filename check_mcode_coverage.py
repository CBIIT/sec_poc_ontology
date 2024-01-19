import wget
import datetime
import argparse
import tempfile
from pathlib import Path
import requests
import psycopg2
import psycopg2.extras
import sqlalchemy
import time
import pandas as pd

pd.set_option('display.max_colwidth', 1000)
parser = argparse.ArgumentParser(description='Download the NCIT Thesaurus Zip file and create transitive closure tables in a sqlite or Postgresql database.')

parser.add_argument('--dbname', action='store', type=str, required=False)
parser.add_argument('--host', action='store', type=str, required=False)
parser.add_argument('--user', action='store', type=str, required=False)
parser.add_argument('--password', action='store', type=str, required=False)
parser.add_argument('--port', action='store', type=str, required=False)
parser.add_argument('--schema', action='store', type=str, required=False)
parser.add_argument('--dbfilename', action='store', type=str, required=False )
#sys.exit()

args = parser.parse_args()

if args.dbfilename is None:
    connection_string = f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.dbname}'
else:
    connection_string = args.dbfilename


if args.dbfilename is None:
    if args.schema is not None:
        sae_connection = sqlalchemy.create_engine(connection_string,
                                                 connect_args={'options': '-c search_path={}'.format(args.schema)}, future=True)
    else:
        sae_connection = sqlalchemy.create_engine(connection_string)
    sa_connection = sae_connection.connect()
    db_connection = psycopg2.connect(database=args.dbname, user=args.user, host=args.host, port=args.port,
                           password=args.password, options="-c search_path={}".format(args.schema))
    con = db_connection
else:
    db_connection = sqlite3.connect(connection_string)
    sa_connection = db_connection
    con = db_connection


cur = db_connection.cursor()


def is_code_reachable(concept_code):
   # print("checking if ", concept_code, "is reachable")
    sql_reachable = "select count(*) as reachable  from umls.ncit_tc_all where parent like 'C%%' and descendant = %s"
    sql_reachable_directly = """
    select count(*) as reachable  from umls.ncit_tc_with_path_all where parent like 'C%%' and descendant = %s and level = 1
    """
    cur.execute(sql_reachable, (concept_code, ) )
    reachable_rs = cur.fetchone()
    cur.execute(sql_reachable_directly, (concept_code,))
    reachable_directly_rs = cur.fetchone()
    return (reachable_rs[0], reachable_directly_rs[0])



print('mCODE coverage analyser')
df = pd.read_excel('mCODEDataDictionary-STU3.xlsx', sheet_name='Value set codes')

code_system_prefixes = {'http://hl7.org/fhir/sid/icd-10-cm': 'ICD10CM',
                        'http://snomed.info/sct':'SNOMEDCT',
                        'http://loinc.org':'LOINC'}



df['prefix'] = df['Code System'].apply( lambda x: code_system_prefixes[x] if x in code_system_prefixes else None )
df['concept_code'] = df.apply(lambda x: x['prefix'] + ":" + str(x['Code']) if x['Code'] is not None and not pd.isna(x['Code']) and x['prefix'] is not None and 'is' not in str(x['Code'])  else None, axis =1 )
reachable_counts = {
    'ICD10CM': {'total':0 , 'reachable':0, 'reachable_directly': 0},
    'SNOMEDCT': {'total':0 , 'reachable':0, 'reachable_directly': 0},
    'LOINC': {'total':0 , 'reachable':0, 'reachable_directly': 0}
}
for ind, row in df.iterrows():
    if row['concept_code'] is not None:
        (reachable, reachable_directly) = is_code_reachable(row['concept_code'])
        if reachable == 0 and reachable_directly == 0:
            print(str(row['concept_code']), 'is not reachable')
            print(row.to_frame())
            print("------------------------------------")
        reachable_counts[row['prefix']]['total'] += 1
        if reachable > 0:
            reachable_counts[row['prefix']]['reachable'] += 1
        if reachable_directly > 0:
            reachable_counts[row['prefix']]['reachable_directly'] += 1

print(reachable_counts)
