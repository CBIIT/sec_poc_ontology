#!/usr/bin/env python
# coding: utf-8



import sqlite3
import traceback

import pandas as pd
import zipfile
import pprint
import sys
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

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]



start_time = datetime.datetime.now()
pp = pprint.PrettyPrinter(indent=4)

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
    print('unified ontology build -- using Postgresql')
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
                                    if syn['subSource'] == 'ICD-10 CM':
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
                                        #print(newc)
                                        #sys.exit()
                chunk_count = chunk_count + 1
                print(datetime.datetime.now(), "processing chunk ", chunk_count, " record count = ", record_count)
                break

    #
    print("returning dataframes")
    new_df = pd.DataFrame(data=new_column_vals, columns=['concept', 'parent', 'path', 'level'])
    return new_df
    #


#load the curated crosswalk table into the DB
crosswalk_df = pd.read_csv('local_crosswalk.csv', delimiter=',', header=0
                          )

crosswalk_df.to_sql('curated_crosswalk', con=sa_connection, if_exists='replace')
sa_connection.commit()
cur.execute('create index cc_code_system_idx on curated_crosswalk(code_system)'
)

sa_connection.commit()
con.commit()

if args.dbfilename is  None:
    cur.execute("""
    create table if not exists ncit_version_composite(
      version_id varchar(32),
      downloaded_url text,
      active_version varchar(1),
      composite_ontology_generation_date text)
    """)
else:
    cur.execute("""
       create table if not exists ncit_version_composite(
         version_id varchar(32),
         downloaded_url text,
         active_version varchar(1),
         composite_ontology_generation_date timestamp)
       """)
con.commit()

url_fstring = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/%s_Release/Thesaurus_%s.FLAT.zip"
r = requests.get('https://api-evsrest.nci.nih.gov/api/v1/concept/ncit',
                 params={'include': 'minimal', 'list': 'C2991'}, timeout=(.4, 7.0))
evs_results = r.json()
print(evs_results)
if len(evs_results) != 1 or 'version' not in evs_results[0]:
    print("NO VERSION NUMBER in returned info from EVS", evs_results)
    sys.exit(0)

current_evs_version = evs_results[0]['version']
print("Current EVS NCIT version :", current_evs_version)
#
# E.g. https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/archive/22.01e_Release/Thesaurus_22.01e.FLAT.zip
# Note that the NCIT Download page MAY have a different version that what EVS is using.  We
# want to stay in sync with EVS
#

check_version_sql = " select version_id from ncit_version_composite where active_version = 'Y'"
cur = con.cursor()
cur.execute(check_version_sql)
rs = cur.fetchone()

#current_evs_version = '23.04d'
if rs is not None and rs[0] == current_evs_version:
    print("POC NCIt is same as EVS NCIt, exiting")
    con.commit()
    con.close()
    sys.exit(0)
elif rs is None:
    print("No current NCIt version noted in the POC db for the composite ontology")
else:
    print("EVS NCIt version :", current_evs_version, " POC DB composite ontology NCIt version:", rs[0])

print("Preparing to process NCIT version", current_evs_version)

url_string = url_fstring % (current_evs_version, current_evs_version)

tfilename = Path(url_string).name
tfile = tempfile.NamedTemporaryFile(suffix=tfilename)
# sys.exit()
thesaurus_zip = wget.download(url=url_string, out=tfile.name)
#thesaurus_zip = 'Thesaurus_23.04d.FLAT.zip'
arch = zipfile.ZipFile(thesaurus_zip, mode='r')

print("Extracting thesaurus file contents")
thesaurus_file = arch.open('Thesaurus.txt', mode='r')

# Names in the dataframe will become columns in the sqlite database

# In[37]:


ncit_df = pd.read_csv(thesaurus_file, delimiter='\t', header=None,
                          names=('code', 'url', 'parents', 'synonyms',
                                 'definition', 'display_name', 'concept_status', 'semantic_type', 'pref_name'))

# Add in the preferred name field - the first choice in the list of synonyms.  Note that this column is not in the NCI Thesaurus and can be computed in pandas or in sqlite.

# In[38]:


ncit_df['pref_name'] = ncit_df.apply(lambda row: row['synonyms'].split('|')[0], axis=1)

# In[39]:


# print(ncit_df)


# In[40]:

# Write the dataframe out to the sqlite table
print("Writing thesaurus file to database")
ncit_df.to_sql('ncit', con=sa_connection, if_exists='replace')
sa_connection.commit()
#con.execute("delete from ncit where concept_status in ('Obsolete_Concept', 'Retired_Concept')")

# In[42]:

print("creating thesaurus file indexes")
cur.execute("drop index if exists ncit_code_index")
cur.execute("create index ncit_code_index on ncit(code)")
cur.execute("drop index if exists lower_pref_name_idx")
cur.execute("create index lower_pref_name_idx on ncit(lower(pref_name))")
con.commit()

# In[43]:


print("getting all concepts that have parents")
#cur = con.cursor()
cur.execute("select code, parents from ncit where (parents is not null and parents <> '')")


# Get all of the concepts that have parents into a result set

# In[44]:


concept_parents = cur.fetchall()

# In[45]:


#print(len(concept_parents))

# In[46]:


con.commit()

# Create a table that will hold the concept, the parent, the path from parent to the concept, and the level (need this to properly recurse along the relationship).

# In[47]:


cur.execute('drop table if exists parents')
cur.execute("""
create table parents (
concept text,
parent text,
path text,
level int)
""")

# Put the direct concept &rarr; parent relationships in the table as level 1 items.

# In[48]:

print("inserting NCIt parents")
for concept, parents in concept_parents:
    parentl = parents.split('|')
    for p in parentl:
        if args.schema is None:
            cur.execute("insert into parents(concept, parent, level, path )values(?,?,1,?)",
                        (concept, p, p + '|' + concept))
        else:
            cur.execute("insert into parents(concept, parent, level, path )values(%s,%s,1,%s)",
                        (concept, p, p + '|' + concept))
con.commit()


# Now put in ICD10CM parents.

print("inserting ICD10-crosswalk parents")
cur.execute("""
insert into parents(concept, parent, level, path )
select  'ICD10CM:' ||  disease_code as concept, evs_nci_code as parent , 1 as level, evs_nci_code || '|' || 'ICD10CM:' || disease_code as path
from curated_crosswalk where code_system = 'ICD10'
""")
con.commit()

print("Inserting ICD10CM parents ")
cur.execute("""
insert into parents(concept, parent, level, path )
select distinct  'ICD10CM:' || d.code as concept,  'ICD10CM:'|| p.code as parent, 1 as level ,  'ICD10CM:'||p.code ||  '|' ||   'ICD10CM:'||d.code as path
from MRHIER  h 
join mrconso p on p.aui = h.paui 
join mrconso d on d.aui = h.aui
and h.sab = 'ICD10CM'
""")
con.commit()

print("inserting loinc parents")
cur.execute("""
insert into parents(concept, parent, level, path )
select  distinct  'LOINC:' || d.code as concept,  'LOINC:'|| p.code as parent, 1 as level ,  'LOINC:'||p.code ||  '|' ||   'LOINC:'||d.code as path
from MRHIER  h 
join mrconso p on p.aui = h.paui 
join mrconso d on d.aui = h.aui
and h.sab = 'LNC'
""")
con.commit()
print("inserting snomedct parents")
cur.execute("""
insert into parents(concept, parent, level, path )
select  distinct  'SNOMEDCT:' || d.code as concept,  'SNOMEDCT:'|| p.code as parent, 1 as level ,  'SNOMEDCT:'||p.code ||  '|' ||   'SNOMEDCT:'||d.code as path
from MRHIER  h 
join mrconso p on p.aui = h.paui 
join mrconso d on d.aui = h.aui
and h.sab = 'SNOMEDCT_US'
""")
con.commit()

# In[49]:

# Now get mCode links

xwalk_df = get_ncit_ehr_syns_for_code() # Loop through all of the NCIt to make sure all of the mCode mappings are captured.

# xwalk_df = get_ncit_ehr_syns_for_code('C192880') # mcode diseases
# #xwalk_df.to_sql(name='parents', con=sa_connection, if_exists='append', index=False)
# con.commit()
# new_xwalk_df = get_ncit_ehr_syns_for_code('C192883') #mcode procedures
# xwalk_df = pd.merge(xwalk_df, new_xwalk_df,  how='left')
# con.commit()
# new_xwalk_df = get_ncit_ehr_syns_for_code('C192884') #mcode fish procedure
# xwalk_df = pd.merge(xwalk_df, new_xwalk_df,  how='left')
# con.commit()

xwalk_df.to_sql(name='parents', con=sa_connection, if_exists='append', index=False)
sa_connection.commit()
con.commit()
#print(con.execute("select * from parents where parent= 'C3824'").fetchall())

# In[50]:

print(new_ehr_subsources)

cur.execute("drop index if exists par_concept_idx")
cur.execute("create index par_concept_idx on parents(concept)")
cur.execute("drop index if exists par_par_idx")
cur.execute("create index par_par_idx on parents(parent)")

# This is the key part - execute the recursive SQL to generate the set of all paths through the NCIt.  We'll prune this to just concepts and descendants a few steps below.

# In[63]:

print(datetime.datetime.now(), "computing transitive closure for NCIt")
cur.execute("drop table if exists ncit_tc_with_path_ncit")
cur.execute(
    """create table ncit_tc_with_path_ncit as with recursive ncit_tc_rows(parent, descendant, level, path ) as 
            (select p1.parent, p1.concept as descendant, p1.level, p1.path from parents p1 where p1.parent like  'C%' and p1.concept like 'C%'  union all 
            select p.parent , n.descendant as descendant, n.level+1 as level ,  p.parent || '|' || n.path  as path 
            from ncit_tc_rows n join parents p on n.parent = p.concept  where p.parent like  'C%' and p.concept like 'C%' 
            ) select * from ncit_tc_rows
            """)
con.commit()

print(datetime.datetime.now(),"computing transitive closure for ICD10CM")
cur.execute("drop table if exists ncit_tc_with_path_icd10cm")
cur.execute(
    """create table ncit_tc_with_path_icd10cm as with recursive ncit_tc_rows(parent, descendant, level, path ) as 
            (select p1.parent, p1.concept as descendant, p1.level, path from parents p1 where p1.parent like  'ICD10CM%' and p1.concept like 'ICD10CM%'  union all 
            select p.parent , n.descendant as descendant, n.level+1 as level ,  p.parent || '|' || n.path  as path 
            from ncit_tc_rows n join parents p on n.parent = p.concept  where p.parent like  'ICD10CM%' and p.concept like 'ICD10CM%'  
            ) select * from ncit_tc_rows
            """)
con.commit()

print(datetime.datetime.now(), "computing transitive closure for SNOMEDCT")
cur.execute("drop table if exists ncit_tc_with_path_snomedct")
cur.execute("""
    create table ncit_tc_with_path_snomedct as with recursive ncit_tc_rows(parent, descendant, level, path ) as 
            (select p1.parent, p1.concept as descendant, p1.level, path from parents p1 where p1.parent like  'SNOMEDCT%' and p1.concept like 'SNOMEDCT%'  union all 
            select p.parent , n.descendant as descendant, n.level+1 as level ,  p.parent || '|' || n.path  as path 
            from ncit_tc_rows n join parents p on n.parent = p.concept  where p.parent like  'SNOMEDCT%' and p.concept like 'SNOMEDCT%'  
            ) select * from ncit_tc_rows
            """)
con.commit()

print(datetime.datetime.now(), "computing transitive closure for LOINC")
cur.execute("drop table if exists ncit_tc_with_path_loinc")
cur.execute("""
    create table ncit_tc_with_path_loinc as with recursive ncit_tc_rows(parent, descendant, level, path ) as 
            (select p1.parent, p1.concept as descendant, p1.level, path from parents p1 where p1.parent like  'LOINC%' and p1.concept like 'LOINC%'  union all 
            select p.parent , n.descendant as descendant, n.level+1 as level ,  p.parent || '|' || n.path  as path 
            from ncit_tc_rows n join parents p on n.parent = p.concept  where p.parent like  'LOINC%' and p.concept like 'LOINC%'  
            ) select * from ncit_tc_rows
            """)

print(datetime.datetime.now(), "computing transitive closure for composite ontology")
# Create a table to bootstrap the parents - it includes the NCIT-><not NCIT> links,
# the first level links into the non ncit ontologies and the first level links 'up' in the NCIT

cur.execute("drop table if exists comp_parents")
cur.execute("""
create table comp_parents as 
select dp.parent, dp.concept , dp.level, dp.path 
from parents dp join parents p1 on p1.concept = dp.parent 
where  p1.parent like  'C%' and p1.concept not like 'C%' 
union 
select dp.parent, dp.concept, dp.level, dp.path 
from parents dp join parents p1 on p1.parent = dp.concept 
where  p1.parent like  'C%' and p1.concept  not like 'C%' 
union 
select p1.parent, p1.concept , p1.level, path from parents p1 where p1.parent like  'C%' and p1.concept not like 'C%' 
""")
con.commit()



cur.execute("drop table if exists ncit_tc_with_path_comp")
cur.execute(
    """create table ncit_tc_with_path_comp as with recursive ncit_tc_rows(parent, descendant, level, path ) as 
           ( select parent, concept as descendant, level, path from comp_parents   union all 
            select p.parent , n.descendant as descendant, n.level+1 as level ,  p.parent || '|' || n.path  as path 
            from ncit_tc_rows n join parents p on n.parent = p.concept 
            ) select * from ncit_tc_rows
            """)

con.commit()

print(datetime.datetime.now(), "creating union of tables")
cur.execute("drop table if exists ncit_tc_with_path_all")
con.commit()

print(datetime.datetime.now(), "adding in NCIt paths")
cur.execute(
    """create table ncit_tc_with_path_all as 
          select parent, descendant, level, path from ncit_tc_with_path_ncit
    """)

con.commit()

print(datetime.datetime.now(), "adding in icd10cm paths")
cur.execute("""
insert into ncit_tc_with_path_all(	parent, descendant, level, path) 
select parent, descendant, level, path from ncit_tc_with_path_icd10cm""")
con.commit()

print(datetime.datetime.now(), "adding in loinc paths")

cur.execute("""
insert into ncit_tc_with_path_all(	parent, descendant, level, path) 
select parent, descendant, level, path from ncit_tc_with_path_loinc
""")
con.commit()

print(datetime.datetime.now(), "adding in snomedct paths")

cur.execute("""
insert into ncit_tc_with_path_all(	parent, descendant, level, path) 
select parent, descendant, level, path from ncit_tc_with_path_snomedct
""")
con.commit()

cur.execute("""
insert into ncit_tc_with_path_all(	parent, descendant, level, path) 
select parent, descendant, level, path from ncit_tc_with_path_comp
""")
con.commit()

print(datetime.datetime.now(), "creating indexes")

cur.execute('create index ncit_tc_path_parent on ncit_tc_with_path_all(parent)')
cur.execute('CREATE INDEX ncit_tc_path_descendant on ncit_tc_with_path_all(descendant)')
con.commit()

# Create the transitive closure table.  This fits the mathematical definition of transitive closure.

# In[52]:

print(datetime.datetime.now(), "creating tc_all tables")

cur.execute('drop table if exists ncit_tc_all')
con.commit()
cur.execute("create table ncit_tc_all as select distinct parent, descendant from ncit_tc_with_path_all ")
con.commit()
cur.execute('create index ncit_tc_parent_all on ncit_tc_all (parent) ')
con.commit()
# In[53]:

rs = cur.execute('select count(*) from ncit_tc_all')
total_num_rows_in_tc = cur.fetchone()[0]
print("There are ", total_num_rows_in_tc, "rows in the transitive closure table")


print(datetime.datetime.now(), "adding in reflexive parent rows")

cur.execute(
    '''with codes as 
    (
    select distinct parent as code from ncit_tc_all
    union
    select distinct descendant as code from ncit_tc_all
    ) 
    insert into ncit_tc_all (parent, descendant) 
    select c.code as parent, c.code as descendant from codes c
    ''')

# In[64]:
con.commit()

print(datetime.datetime.now(), "adding in reflexive paths")

cur.execute(
    '''with codes as 
    (
    select distinct parent as code from ncit_tc_all
    union
    select distinct descendant as code from ncit_tc_all
    ) 
    insert into ncit_tc_with_path_all (parent, descendant, level, path) 
    select c.code, c.code, 0  , c.code  from codes c
    ''')

# In[59]:
con.commit()
cur.execute("drop index if exists tc_desc_all_index")
con.commit()
cur.execute("create index tc_desc_all_index on ncit_tc_all(descendant)")
con.commit()
cur.execute("drop index if exists tc_parent_all_index")
con.commit()
cur.execute("create index tc_parent_all_index on ncit_tc_all(parent )")
con.commit()

rc = cur.execute("select count(*) from ncit_tc_all where parent=descendant")
reflexive_concepts = cur.fetchone()[0]
# In[60]:


print('There are ', reflexive_concepts, ' reflexive relationships added to the transitive closure.')

# In[61]:

rc = cur.execute('select count(*) from ncit_tc_all')
total_num_rows_in_tc = cur.fetchone()[0]
print("There are ", total_num_rows_in_tc, "rows in the transitive closure table")

rc = cur.execute("select count(*) from ncit_tc_with_path_all ")
num_paths = cur.fetchone()[0]
print("There are ", num_paths , " distinct paths in the composite ontology.")

# In[62]:




con.commit()


cur.execute('update ncit_version_composite set active_version = NULL')
if args.schema is None:
    cur.execute("""insert into ncit_version_composite(version_id, downloaded_url,composite_ontology_generation_date, active_version )
           values(?,?,?,?)
        """, [current_evs_version, 'hardcoded', datetime.datetime.now(), 'Y'])
else:
    cur.execute("""insert into ncit_version_composite(version_id, downloaded_url,composite_ontology_generation_date, active_version )
           values(%s,%s,%s,%s)
        """, [current_evs_version, 'hardcoded', datetime.datetime.now(), 'Y'])
con.commit()

end_time = datetime.datetime.now()
print("Process complete in ", end_time - start_time)

con.close()
