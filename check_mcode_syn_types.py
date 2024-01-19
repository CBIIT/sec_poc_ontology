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

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


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
new_ehr_subsources = set()

def get_ncit_ehr_syns_for_code(code=None):

    if code is None:
        cur.execute("select code from ncit")
    else:
        cur.execute("select descendant from ncit_tc where parent = ? and descendant like 'C%' ", [code])

    concept_rs = cur.fetchall()
    concept_list = [r[0] for r in concept_rs]

    num_concepts_per_evs_call = 575
   # concept_list = ncit_df['code'].tolist()

    concept_url_fstring = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit?list=%s&include=summary"
    new_column_vals = []

    chunk_count = 0
    record_count = 0
    retry_limit = 3

    print("Calling EVS to get crosswalk terms")
    for ch in chunks(concept_list, num_concepts_per_evs_call):
        c_codes = list(ch)
        record_count += len(c_codes)
        c_codes_string = ','.join(c_codes)
        #print(c_codes_string)
        concept_url_string = concept_url_fstring % (c_codes_string)
        retry_count = 0

        while retry_count < retry_limit:
            try:
                r = requests.get(concept_url_string, timeout=(1.0, 15.0))
            except requests.exceptions.RequestException as e:
                print("exception -- ", e)
                print("sleeping")
                retry_count += 1
                if retry_count == retry_limit:
                    print("retry max limit hit -- bailing out ")
                    sys.exit()
                time.sleep(15)
            else:
                try:
                    concept_set = r.json()
                except Exception:
                    print("ERROR in ", concept_url_string)
                    traceback.print_exc()
                    sys.exit()
                for newc in concept_set:
                    #new_column_vals.append((newc['code'], newc['name']))
                    if 'synonyms' in newc:
                        for syn in newc['synonyms']:
                            if 'source' in syn and syn['source'] == 'mCode':
                              #  print("found mcode :",newc)
                                if 'subSource' in syn:
                                    if syn['subSource'] in ('ICD-10 CM', 'ICD-10-CM'):
                                      #  print('adding ' + newc['code'] + ' => ', 'ICD10CM:'+syn['code'])
                                        new_column_vals.append(('ICD10CM:'+syn['code'],
                                                               newc['code'],
                                                               newc['code']+'|'+'ICD10CM:'+syn['code'],
                                                               1))
                                    elif syn['subSource'] == 'LOINC':
                                      #  print('adding ' + newc['code'] + ' => ', 'LOINC:' + syn['code'])
                                        new_column_vals.append(('LOINC:' + syn['code'],
                                                               newc['code'],
                                                               newc['code'] + '|' + 'LOINC:' + syn['code'],
                                                               1))
                                    elif syn['subSource'] == 'SNOMED CT':
                                      #  print('adding ' + newc['code'] + ' => ', 'SNOMEDCT:' + syn['code'])
                                        new_column_vals.append(('SNOMEDCT:' + syn['code'],
                                                               newc['code'],
                                                               newc['code'] + '|' + 'SNOMEDCT:' + syn['code'],
                                                               1))
                                    else:
                                        new_ehr_subsources.add(syn['subSource'])
                                        print(str(newc))
                                        #print(newc)
                                        #sys.exit()
                chunk_count = chunk_count + 1
                print(datetime.datetime.now(), "processing chunk ", chunk_count, " record count = ", record_count)
                break

    #
    print("returning dataframes")
    print(new_ehr_subsources)
    new_df = pd.DataFrame(data=new_column_vals, columns=['concept', 'parent', 'path', 'level'])
    return new_df
    #

df = get_ncit_ehr_syns_for_code()