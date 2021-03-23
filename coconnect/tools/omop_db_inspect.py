import json
import os
import sqlalchemy as sql
import pandas as pd
import numpy as np

class OMOPDetails():
    def __init__(self):
        db_name = os.environ['OMOP_POSTGRES_DB']
        db_user = os.environ['OMOP_POSTGRES_USER']
        db_password = os.environ['OMOP_POSTGRES_PASSWORD']
        db_host = os.environ['OMOP_POSTGRES_HOST']
        db_port = int(os.environ['OMOP_POSTGRES_PORT'])
        #need special format for azure
        #https://github.com/MicrosoftDocs/azure-docs/issues/6371#issuecomment-376997025
        con_str =f'postgresql://{db_user}%40{db_host}:{db_password}@{db_host}:{db_port}/{db_name}'
        self.ngin = sql.create_engine(con_str)

        self.inspector = sql.inspect(self.ngin)
        self.schema = 'public'

        dir_path = os.path.dirname(os.path.realpath(__file__))
        _version = 'v5_3_1'
        f_path = f'{dir_path}/../data/cdm/OMOP_CDM_{_version}.csv'
        self.cdm = pd.read_csv(f_path,encoding="ISO-8859-1")\
                     .set_index('table')[['field']]
        
        #self.omop_tables = [
        #    table
        #    for table in self.inspector.get_table_names(schema=self.schema)
        #]
        #self.omop_tables.sort()

    def get_rules(self,source_concept_ids):
        #From OMOP db get concept relationship
        select_from_concept = r'''
        SELECT *
        FROM public.concept
        WHERE concept_id IN (%s)
        '''
        select_from_concept_relationship = r'''
        SELECT *
        FROM public.concept_relationship
        WHERE concept_id_1 IN (%s)
        '''
        
        if isinstance(source_concept_ids,int):
            source_concept_ids = {None: source_concept_ids}
            
        _ids = ",".join([
            str(x)
            for x in source_concept_ids.values()
        ])
        
        df_concept = pd.read_sql(
            select_from_concept%(_ids),self.ngin)\
                       .drop(
                           [
                               "valid_start_date",
                               "valid_end_date",
                               "invalid_reason"
                           ]
                           ,axis=1)
        df_relationship = pd.read_sql(
            select_from_concept_relationship%(_ids),self.ngin)\
                            .drop(
                                ["valid_start_date",
                                 "valid_end_date",
                                 "invalid_reason"],axis=1)

        relationship_ids = [
            #'Mapped from',
            'Concept same_as to',
            'Maps to']
        df_relationship = df_relationship[
            df_relationship['relationship_id'].isin(relationship_ids)
        ]
        df_concept.set_index('concept_id',inplace=True)
        df_relationship.set_index('concept_id_1',inplace=True)

        
        info = df_concept.join(df_relationship).reset_index()
        info['domain_id'] = info['domain_id'].str.lower()
        domains = info['domain_id'].unique()
        #cond = self.cdm['field'].str.contains(domain) \
        #    & self.cdm['field'].str.contains('concept_id')

        cols = ['concept_id', 'concept_name', 'domain_id', 'vocabulary_id',
                'concept_class_id', 'standard_concept', 'concept_code',
                'concept_id_2','relationship_id']
        info = info[['concept_id','concept_id_2','domain_id']]
        info.set_index('domain_id',inplace=True)
        info.columns = ['source_concept_id','concept_id']

        temp = pd.DataFrame.from_dict(source_concept_ids,
                                      columns=['source_concept_id'],
                                      orient='index')
        temp.index.rename('source_value',inplace=True)
        temp.reset_index(inplace=True)
        info = info.reset_index().merge(temp,
                                        left_on='source_concept_id',
                                        right_on='source_concept_id')\
                                 .set_index('domain_id')

        domain_id = info.index.unique()[0]

        if len(info.index.unique()) > 1:
            print ('fuck off')
            exit(0)

        info.columns = [f"{domain_id}_{col}" for col in info.columns]
        temp = info.loc[[domain_id]]\
                   .reset_index(drop=True)\
                   .set_index(f"{domain_id}_source_value")\
                   .T\
                   .astype('Int64')\
                   .fillna(np.NaN)\
                   .astype(str)
        
        temp = {
            x:temp.loc[x].to_dict()
            for x in temp.index
        }

        #convert None to scalar for field level mapping
        for k,v in temp.items():
            if None in v:
                temp[k] = v[None]
        
        temp[f"{domain_id}_source_value"] = None
        return temp

        
#         relationships=df_relationship['relationship_id'].tolist()
#         #1)Check if source_concept_id is Standard or Non-standard
#         #2)Get the relevant target table for the source_concept_id
#         for relationship in relationships:
#             if relationship=="Mapped from":
#                 self.is_standard="Standard"
#                 self.target_concept_id=df_relationship['concept_id_1'].iloc[relationships.index(relationship)]
#                 self.source_concept_id=self.target_concept_id
#                 self.target_table = df_concept['domain_id'].iloc[relationships.index(relationship)]
#             elif relationship=="Concept same_as to":
#                 self.is_standard="Non-Standard"
#                 self.source_concept_id=df_relationship['concept_id_1'].iloc[relationships.index(relationship)]
#                 self.target_concept_id=df_relationship['concept_id_2'].iloc[relationships.index(relationship)]
#                 self.target_table = df_concept['domain_id'].iloc[0]

if __name__ == '__main__':
    # concept_id=4060225
    from dotenv import load_dotenv
    load_dotenv()
    tool = OMOPDetails()
    print (tool.get_rules(37399052))
    exit(0)
    print (tool.get_rules({'M':8507,'F':8532}))
    print (tool.get_rules({"BLACK CARIBBEAN": 4087917, "ASIAN OTHER": 4087922, "INDIAN": 4185920, "WHITE BRITISH": 4196428}))

    print (tool.get_rules({'0.2':37398191,'0.4':37398191}))
    
    
    #print (tool.get_info(4060225))
# '''# 8507 8532
