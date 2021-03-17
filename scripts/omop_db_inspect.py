import json
import os
import sqlalchemy as sql
import pandas as pd

from dotenv import load_dotenv
load_dotenv()


db_name = os.environ['POSTGRES_DB']
db_user = os.environ['POSTGRES_USER']
db_password = os.environ['POSTGRES_PASSWORD']
db_host = os.environ['POSTGRES_HOST']
db_port = int(os.environ['POSTGRES_PORT'])


#need special format for azure
#https://github.com/MicrosoftDocs/azure-docs/issues/6371#issuecomment-376997025
con_str =f'postgresql://{db_user}%40{db_host}:{db_password}@{db_host}:{db_port}/{db_name}'

ngin = sql.create_engine(con_str)
inspector = sql.inspect(ngin)

schema = 'public'
omop_tables = [
    table
    for table in inspector.get_table_names(schema=schema)
]
omop_tables.sort()

print (json.dumps(omop_tables,indent=6))


for _id in ['8507','8532','4057304','36716287','37398192','378253']:
    selection = r'''
    SELECT *
    FROM public.concept
    WHERE concept_id=%s
    '''
    df = pd.read_sql(selection%(_id),ngin).drop(["valid_start_date","valid_end_date","invalid_reason"],axis=1)
    print ()
    print (df.T)
    class_id = df['concept_class_id'].iloc[0]
    domain_id = df['domain_id'].iloc[0]
    #print (class_id)
    #print (domain_id)
exit(0)

selection = r'''
SELECT *
FROM public.concept_synonym
WHERE concept_id=%s
'''# 8507 8532
df = pd.read_sql(selection%('8507'),ngin)
print (df)

selection = r'''
SELECT *
FROM public.domain
WHERE domain_id LIKE '%s'
'''%(f'%%{domain_id}%%')
df = pd.read_sql(selection,ngin)
print (df)


selection = r'''
SELECT *
FROM public.location_history
'''
df = pd.read_sql(selection,ngin)
print (df)
exit(0)



selection = r'''
SELECT *
FROM public.concept_class
WHERE concept_class_id LIKE '%s';
'''%(f"%%{class_id}%%")

df = pd.read_sql(selection,ngin)
print (df.columns)
print (df)
print ()


for table in filter(lambda tab: 'concept' in tab, omop_tables):
    selection = r'''
    SELECT *
    FROM public.%s
    LIMIT 1
    '''%(table)
    df = pd.read_sql(selection,ngin)
    print ({table:df.columns})



def get_full_df(table,schema=schema):
    selection = r'''
    SELECT *
    FROM %s.%s
    LIMIT 10
    '''%(schema,table)
    return pd.read_sql(selection,ngin)

#print (get_full_df('person').columns.values)
#print (get_full_df('source_to_concept_map'))

dict={}
for table in omop_tables:
    dict[table] = list(get_full_df(table).columns.values)

print (json.dump(dict,open('info.json','w'),indent=6))




#omop_fields = {
#    table: get_df(table).columns
#    for table in omop_tables
#}
#print (omop_fields)
