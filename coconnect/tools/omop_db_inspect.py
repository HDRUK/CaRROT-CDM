import json
import os
from pandas.io.formats.format import IntArrayFormatter
import sqlalchemy as sql
import pandas as pd
import numpy as np
from collections import defaultdict


class MultipleDomainsForInputConcepts(Exception):
    pass
class MultipleStandardMappingsForInputConcepts(Exception):
    pass





class OMOPDetails():

    #Get the directory of the OMOP_CDM.csv file
    #Save it into a pandas dataframe
    #Return the dataframe
    @classmethod
    def to_df(self,_version = 'v5_3_1'):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        f_path = f'{dir_path}/../data/cdm/OMOP_CDM_{_version}.csv'
        self.cdm = pd.read_csv(f_path,encoding="ISO-8859-1")\
                     .set_index('table')[['field']]
        
        return self.cdm
    
    #Initialise the database connection to OMOP_POSTGRES_DB
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

        
        #self.omop_tables = [
        #    table
        #    for table in self.inspector.get_table_names(schema=self.schema)
        #]
        #self.omop_tables.sort()

    """ get_rules() method: 
        1)Queries the DB, gets the contents of two tables: concept & concept_relationship
        and puts the contents into two pandas dataframes.
        2) Checks if the conceptID is standard/non-standard
    """
    def get_rules(self,source_concept_ids):
        print ("working on",source_concept_ids)
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

        #when the relationship=Maps to -> Non-standard to standard mapping
        #we don't need to check for Concept same_as_to
        relationship_ids = [
            'Maps to']
        #only keep the rows for the relationship_id being in the above list
        df_relationship = df_relationship[
            df_relationship['relationship_id'].isin(relationship_ids)
        ]
        
        #do some indexing, so we can join the two dataframes
        df_concept.set_index('concept_id',inplace=True)
        df_relationship.set_index('concept_id_1',inplace=True)
        #join em
        info = df_concept.join(df_relationship)
        info.index.rename('concept_id',inplace=True)
        info = info.reset_index()
        print("Info after merge")
        print(info)
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
        #rename concept_id_2 --> concept_id
        info.columns = ['source_concept_id','concept_id']
        
        #temp dataframe to help handle source values
        temp = pd.DataFrame.from_dict(source_concept_ids,
                                      columns=['source_concept_id'],
                                      orient='index')

        temp.index.rename('source_value',inplace=True)
        temp.reset_index(inplace=True)
       
        if len(info.concept_id.unique())>1:
            print("There is more than one standard mapping for this source concept ID")
            print(info.concept_id.items)

        #for now not raising an exception until we decide if we handle the two standard maps
        #     raise MultipleStandardMappingsForInputConcepts(
        #         f"{info.concept_id.items}\n"
        #         f"{source_concept_ids} \n"
        #         "There is more than one standard mapping for this source concept ID"
        #         )
           
        #merge with the info table so now we have source_concept_id
        info = info.reset_index().merge(temp,
                                        left_on='source_concept_id',
                                        right_on='source_concept_id')\
                                 .set_index('domain_id')
        print("now with source_concept id")
        print(info)
       
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
        #first column is a dictionary key
        #second column is the value
        info_dict=info.to_dict('index')
        
        #convert None to scalar for field level mapping
        # {None: 12345} --> 12345
        # the key is None if we're not mapping values but the whole field
        #again, trust me
        for k,v in info_dict.items():
            if None in v:
                info_dict[k] = v[None]

        #source_value shouldnt get mapped
        #so return this info
        info_dict[f"{domain_id}_source_value"] = None
        
        return info_dict

        
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
    
