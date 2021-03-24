import json
import os
import sqlalchemy as sql
import pandas as pd
import numpy as np

class MultipleDomainsForInputConcepts(Exception):
    pass


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

        #little trick for handling single concept ids
        if isinstance(source_concept_ids,int):
            source_concept_ids = {None: source_concept_ids}
        if isinstance(source_concept_ids,str):
            source_concept_ids = {None: source_concept_ids}

        #convert the list of concept ids into something the read_sql can handle
        #aka a joined list
        _ids = ",".join([
            str(x)
            for x in source_concept_ids.values()
        ])

        #retrieve the concept_id mapping
        df_concept = pd.read_sql(
            select_from_concept%(_ids),self.ngin)\
                       .drop(#drop some useless shit
                           [
                               "valid_start_date",
                               "valid_end_date",
                               "invalid_reason"
                           ]
                           ,axis=1)

        #retrieve a relationship lookup
        df_relationship = pd.read_sql(
            select_from_concept_relationship%(_ids),self.ngin)\
                            .drop(
                                ["valid_start_date",
                                 "valid_end_date",
                                 "invalid_reason"],axis=1)

        #this needs some work....
        relationship_ids = [
            #'Mapped from',
            'Concept same_as to',
            'Maps to']
        #only keep the rows for the relationship_id being in the above list
        df_relationship = df_relationship[
            df_relationship['relationship_id'].isin(relationship_ids)
        ]

        #do some indexing, so we can join the two dataframes
        df_concept.set_index('concept_id',inplace=True)
        df_relationship.set_index('concept_id_1',inplace=True)

        #join em
        info = df_concept.join(df_relationship).reset_index()
        #lower the domain id so it matches the output omop names
        #e.g. Gender --> gender
        info['domain_id'] = info['domain_id'].str.lower()

        #get a list of unique domain names
        domains = info['domain_id'].unique()

        #could look up the associated tables here...
        #turned off for now, but could be used again
        #cond = self.cdm['field'].str.contains(domain) \
        #    & self.cdm['field'].str.contains('concept_id')

        #just saving what columns there are 
        #cols = ['concept_id', 'concept_name', 'domain_id', 'vocabulary_id',
        #        'concept_class_id', 'standard_concept', 'concept_code',
        #       'concept_id_2','relationship_id']

        #only select what's needed for now
        info = info[['concept_id','concept_id_2','domain_id']]
        #index on domain_id
        info.set_index('domain_id',inplace=True)

        #rename concept_id --> source_concept_id
        #renae concept_id_2 --> concept_id
        #is this right??
        info.columns = ['source_concept_id','concept_id']

        #temp dataframe to help handle source values
        temp = pd.DataFrame.from_dict(source_concept_ids,
                                      columns=['source_concept_id'],
                                      orient='index')
        #some shit, trust me
        temp.index.rename('source_value',inplace=True)
        temp.reset_index(inplace=True)
        
        #merge with the info table so now we have source_concept_id
        info = info.reset_index().merge(temp,
                                        left_on='source_concept_id',
                                        right_on='source_concept_id')\
                                 .set_index('domain_id')

        #raise an error if there are somehow multiple domain_ids for the input concepts
        if len(info.index.unique()) > 1:
            raise MultipleDomainsForInputConcepts(
                f"{info.index.unique().tolist()}\n"
                f"{source_concept_ids} \n"
                "Somehow your concept_ids are associated with different domain_ids"
                )
            
        #get the domain_id
        domain_id = info.index.unique()[0]

        #prepend the domain_id (e.g. gender) to the name of each column
        info.columns = [f"{domain_id}_{col}" for col in info.columns]

        #some playing around, converting/pivoting the dataframe
        #so that we generate multiple rules
        info = info.loc[[domain_id]]\
                   .reset_index(drop=True)\
                   .set_index(f"{domain_id}_source_value")\
                   .T\
                   .astype('Int64')\
                   .fillna(np.NaN)\
                   .astype(str)
        
        #make into a dictionary
        info = {
            x:info.loc[x].to_dict()
            for x in info.index
        }

        #convert None to scalar for field level mapping
        # {None: 12345} --> 12345
        # the key is None if we're not mapping values but the whole field
        #again, trust me
        for k,v in info.items():
            if None in v:
                info[k] = v[None]

        #source_value shouldnt get mapped
        #so return this info
        info[f"{domain_id}_source_value"] = None
        return info

        
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
    from dotenv import load_dotenv
    load_dotenv()
    tool = OMOPDetails()
    print (tool.get_rules(37399052))
    print (tool.get_rules({'M':8507,'F':8532}))
    print (tool.get_rules({"BLACK CARIBBEAN": 4087917, "ASIAN OTHER": 4087922, "INDIAN": 4185920, "WHITE BRITISH": 4196428}))

    print (tool.get_rules({'0.2':37398191,'0.4':37398191}))
    
