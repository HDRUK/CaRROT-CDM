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
    def from_csv(self,_version = 'v5_3_1'):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        f_path = f'{dir_path}/../data/cdm/OMOP_CDM_{_version}.csv'
        self.cdm = pd.read_csv(f_path,encoding="ISO-8859-1")\
                     .set_index('table')[['field']]        
        return self.cdm

    #instead get the cdm objects (destination table & field) from the OMOPDB
    def load_cdm_from_db(self):

        cdm = []
        print ('loading tables')
        #loop over all tables in the OMOP-db
        for table in self.inspector.get_table_names(schema=self.schema):
            #print (self.inspector.get_columns(table_name=table,schema=self.schema))
            #grab the table and put it in a dataframe
            #limit this to 1 row to not take forever to load this up
            df = pd.read_sql('SELECT * FROM %s.%s LIMIT 1'%(self.schema,table),self.ngin)
            #in the omopdb, all cdm objects have length zero
            #anything else is something else e.g concept look up
            if len(df) > 0 : continue

            #get all the column names as a list
            cols = df.columns.tolist()
            print ('loaded',table,'with columns',cols)

            #create an index for each column value to be the table name
            index =[
                table
                for _ in range(len(cols))
            ]

            #add as a dataframe to an array for each table
            # will look like this....
            #                                             field
            #attribute_definition       attribute_definition_id
            #attribute_definition                attribute_name
            #attribute_definition         attribute_description
            #attribute_definition     attribute_type_concept_id
            #attribute_definition              attribute_syntax
            cdm.append(
                pd.DataFrame(cols,
                             index,
                             columns=['field'],
                )
            )

        #merge them together and return this as the cdm lookup frame
        cdm = pd.concat(cdm)
        cdm.index.name = 'table'

        dir_path = os.path.dirname(os.path.realpath(__file__))
        f_path = f'{dir_path}/../data/cdm/OMOP_CDM_ONLINE_LATEST.csv'
        cdm.to_csv(f_path)
        
        

    
    #Initialise the database connection to OMOP_POSTGRES_DB
    def __init__(self,load_from_db=False):
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

        #force the recreation of the cdm csv dump from the omopdb
        if load_from_db:
            self.load_cdm_from_db()
            
        self.cdm = self.from_csv(_version='ONLINE_LATEST')

                   
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
    
    def lookup_code(self, concept_code):
        
        print ("Working on...", concept_code)
        
        # SQL code for looking up codes and conceptIDs
        select_from_code = r'''
        SELECT *
        FROM public.concept
        WHERE concept_code = '%s'
        '''
        select_from_concept_relationship = r'''
        SELECT *
        FROM public.concept_relationship
        WHERE concept_id_1 = ('%s')
        '''
    
        # Retrieve the concept *code* mapping (not the conceptID)
        df_code = pd.read_sql(
            select_from_code%(concept_code),self.ngin
            )
        
        # Look up each concept *code* for a corresponding conceptID
        # Returns only rows where relationship_id == 'Maps to'
        # This is OMOPs weird way of flagging which concept ID is std and valid
        maps = []
        for index, row in df_code.iterrows():
            x = pd.read_sql(select_from_concept_relationship%(row['concept_id']),self.ngin).drop(
                ['valid_start_date',
                 'valid_end_date',
                 'invalid_reason'], 
                axis=1
            )
            relationship_ids = ['Maps to']
            x = x[x['relationship_id']
                  .isin(relationship_ids)]
            maps.append(x)
            
        # Concat maps list into single pandas df
        df_maps = pd.concat(maps,ignore_index=True)
        
        # Join the first df_codes dataframe to the mapped codes for return
        joined_df = df_code.merge(
            df_maps, left_on='concept_id', right_on='concept_id_1'
            )
        
        return joined_df
        
    
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
        #plus check if domain_id is a measurement
        #define is_measurement boolean variable for later checks
        if info.domain_id.values[0]=='measurement':
             info.insert(2, column='value_as_number', value='')
             info=info[['concept_id','concept_id_2','value_as_number','domain_id']]
             is_measurement=True
        else:
            info = info[['concept_id','concept_id_2','domain_id']]
            is_measurement=False
        #index on domain_id
        info.set_index('domain_id',inplace=True)
        
        #rename concept_id --> source_concept_id
        #rename concept_id_2 --> concept_id
        if is_measurement:
            info.columns = ['source_concept_id','concept_id','value_as_number']
        else:
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
        #when value_as_number is present do not prepend domain_id
        if is_measurement: info.rename(columns={f"{domain_id}_value_as_number":'value_as_number'}, inplace=True)

        #some playing around, converting/pivoting the dataframe
        #so that we generate multiple rules
        #if the rule is for measurement do not cast to int64
        if is_measurement:
            info = info.loc[[domain_id]]\
                    .reset_index(drop=True)\
                    .set_index(f"{domain_id}_source_value")\
                    .T\
                    .fillna(np.NaN)\
                    .astype(str)
        else:
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
        info=info.to_dict('index')
        
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
        #check if measurement and set value_as_number to None
        if is_measurement: 
            info["value_as_number"] = None

        contained_within = self.cdm['field'][
            self.cdm['field']\
            .str\
            .contains(f"{domain_id}_source_value")
        ].index.unique().tolist()

        retval = {}
        for table in contained_within:
            retval[table] = info
        
        return retval


    def get_fields(self,domains):
        if isinstance(domains,str):
            return self.cdm.loc[domains]['field'].tolist()
        else:
            return {x:self.cdm.loc[x]['field'].tolist() for x in domains}

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    tool = OMOPDetails(load_from_db=False)
    #print (tool.get_rules(37399052))
    rules = tool.get_rules({'M':8507,'F':8532})
    print (json.dumps(rules,indent=6))
    #print (tool.get_fields(list(rules.keys())))
    #print (tool.get_rules({"BLACK CARIBBEAN": 4087917, "ASIAN OTHER": 4087922, "INDIAN": 4185920, "WHITE BRITISH": 4196428}))
    #print (tool.get_rules({'0.2':37398191,'0.4':37398191}))