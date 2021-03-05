import os
import pandas as pd
import numpy as np
import json
import copy
import collections

from .operations import OperationTools


class CommonDataModel:

    def __init__(self,_start_on_init=True):
        if _start_on_init:
            self.tools = OperationTools()
            self.__dict__.update(self.__class__.__dict__)
            self.finalise()
            
    @classmethod
    def from_mapping_pipeline(self,inputs,f_structural_mapping,is_synthetic=False):
        self.tools = OperationTools()
        self.df_structural_mapping = pd.read_csv(f_structural_mapping)

        if is_synthetic:
            for col in ['source_table','source_field']:
                self.df_structural_mapping[col] = self.df_structural_mapping[col].str.lower()

        
        self.df_structural_mapping.set_index('destination_table',inplace=True)
        destination_tables = self.df_structural_mapping.index.unique()

        _map = {}
        
        for destination_table in destination_tables:
            cls = self.get_cdm_class(self,destination_table)
            if cls is None:
                continue

            _map[destination_table] = {}

            rules = self.df_structural_mapping.loc[destination_table]#.set_index(['destination_field','rule_id'])
            values = rules['destination_field'].value_counts()
            unique_values = sorted(values.unique())
            if len(unique_values) > 2:
                print (values)
                print ("something really wrong")
                exit(0)

            rules.set_index('destination_field',inplace=True)
            initial = values[values==1].index
            for destination_field in initial:
                rule = rules.loc[destination_field]
                source_table = rule['source_table']
                source_field = rule['source_field']
                obj = {'source_table':source_table,
                       'source_field':source_field}
                _map[destination_table][destination_field] = obj

            if len(unique_values) == 2:
                duplicates = values[values==unique_values[1]].index
                for i,destination_field in enumerate(duplicates):
                    rule = rules.loc[destination_field]
                    source_tables = rule['source_table']
                    source_fields = rule['source_field']

                    
                    for j,(source_table,source_field) in enumerate(zip(source_tables,source_fields)):
                        obj = {'source_table':source_table,
                               'source_field':source_field}

                        if i == 0:
                            _map[f"{destination_table}_{j}"] = copy.copy(_map[destination_table])
                        _map[f"{destination_table}_{j}"][destination_field] = obj

        print (json.dumps(_map,indent=4))
        exit(0)
        return self(False)

    def apply_term_map(self,f_term_mapping):
        self.df_term_mapping = pd.read_csv(f_term_mapping)

        self.df_term_mapping = self.df_term_mapping.set_index('rule_id').sort_index()
        self.df_structural_mapping= self.df_structural_mapping\
            [self.df_structural_mapping['term_mapping'].str.contains('y')].reset_index().set_index('rule_id').sort_index()
        
        maps = self.df_term_mapping.join(self.df_structural_mapping)\
                                   .set_index(['destination_table','destination_field'])\
                                   [['source_term','destination_term','term_mapping']].sort_index()

        
        for p in self.get_objs(Person):
            person_map = maps.loc['person']
            for destination_field in person_map.index.unique():
                term_mapper = maps.loc[p.name,destination_field]\
                             .reset_index(drop=True)\
                             .set_index('source_term')['destination_term']\
                             .to_dict()
                print ('mapping',destination_field,'with',term_mapper)
                print (maps.loc[p.name,destination_field])
                mapped_field = getattr(p,destination_field).map(term_mapper)
                setattr(p,destination_field,mapped_field)
                
            exit(0)
        

    def get_cdm_class(self,class_type):
        if class_type in _classes:
            return _classes[class_type]()

    
    def get_objs(self,class_type):
        return  [
            getattr(self,x)
            for x in dir(self)
            if isinstance(getattr(self,x),class_type)
        ]
    
    def run_cdm(self,class_type):
        objects = self.get_objs(class_type)

        if len(objects) == 0:
            return
        
        #execute them all
        dfs = []
        for obj in objects:
            obj.execute(self)
            df = obj.get_df()
            if len(df) == 0:
                continue
            dfs.append(df)

        #merge together
        df_destination = pd.concat(dfs,ignore_index=True)
        df_destination = class_type.finalise(class_type,df_destination)
        
        return df_destination

        
    def finalise(self,f_out='output_data/'):
        self.df_map = {}
        self.df_map[Person.name] = self.run_cdm(Person)
        self.df_map[ConditionOccurrence.name] = self.run_cdm(ConditionOccurrence)

        self.save_to_file(self.df_map,f_out)
        
    def save_to_file(self,df_map,f_out):
        for name,df in df_map.items():
            if df is None:
                continue
            fname = f'{f_out}/{name}.csv'
            print (f'saving to {name} to {fname}')
            df.set_index(df.columns[0],inplace=True)
            df.to_csv(fname,index=True)
            print (df.dropna(axis=1,how='all'))
        


class Base(object):
    def __init__(self,_type):
        self.name = _type
        self.tools = OperationTools()
        #load the cdm
        #get the field name, if it's required and the data type
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.cdm = pd.read_csv(f'{self.dir_path}/../data/cdm/OMOP_CDM_v5_3_1.csv',encoding="ISO-8859-1")\
                     .set_index('table')\
                     .loc[_type].set_index('field')[['required', 'type']]
        
        #save the field names
        self.fields = self.cdm.index.values

        #create new attributes for all the fields in the CDM
        for field in self.fields:
            setattr(self,field,None)

    #default finalise does nothing
    def finalise(self,df):
        return df
            
    def define(self,_):
        return self
            
    def get_destination_fields(self):
        return list(self.fields)

    def execute(self,this):
        self.__dict__.update(this.__dict__)
        self = self.define(self)

    def get_df(self):
        dfs = {
            key: getattr(self,key).rename(key)
            for key in self.fields
            if getattr(self,key) is not None
        }
        if len(dfs) == 0:
            return None
        
        
        df = pd.concat(dfs.values(),axis=1)

        missing_fields = set(self.fields) - set(df.columns)
        for field in missing_fields:
            df[field] = np.NaN

        df = df[self.fields]
        
        
        return df

            
            
class Person(Base):
    name = 'person'
    def __init__(self):
        super().__init__(self.name)
                
    def get_df(self):
        df = super().get_df()
        #convert these key fields
        df['year_of_birth'] = self.tools.get_year(df['year_of_birth'])
        df['month_of_birth'] = self.tools.get_month(df['month_of_birth'])
        df['day_of_birth'] = self.tools.get_day(df['day_of_birth'])
        df['birth_datetime'] = self.tools.get_datetime(df['birth_datetime'])
        return df


class ConditionOccurrence(Base):
    name = 'condition_occurrence'
    def __init__(self):
        super().__init__(self.name)
        

    def finalise(self,df):
        df = df.sort_values('person_id')
        if df['condition_occurrence_id'].isnull().any():
            df['condition_occurrence_id'] = df.reset_index(drop=True).reset_index()['index']
                        
        return df
        
    def get_df(self):
        df = super().get_df()
        #require the condition_concept_id to be filled
        df = df[df['condition_concept_id'].notnull()]

        return df

_classes = {
    'person' : Person,
    'condition_occurrence' : ConditionOccurrence
}
