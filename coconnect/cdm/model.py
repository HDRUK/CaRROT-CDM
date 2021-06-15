import os
import pandas as pd
import numpy as np
import json
import copy

from .operations import OperationTools
from coconnect.tools.logger import Logger
from .objects import Person, ConditionOccurrence, VisitOccurrence, Measurement, Observation

class NoInputFiles(Exception):
    pass

class CommonDataModel:

    inputs = None
    output_folder = "output_data/"
    
    def __init__(self,**kwargs):

        name = self.__class__.__name__
        if 'name' in kwargs:
            name = kwargs['name']

        self.logger = Logger(self.__class__.__name__)
        self.logger.info("CommonDataModel created")

        if 'output_folder' in kwargs:
            self.output_folder = kwargs['output_folder']
        
        if 'inputs' in kwargs:
            inputs = kwargs['inputs']
            if not isinstance(inputs,dict):
                self.logger.error(inputs)
                raise NoInputFiles("setting up inputs that are not a dict!!")

            if not self.inputs is None:
                self.logger.waring("overwriting inputs")

            self.inputs = inputs

        if self.inputs == None:
            raise NoInputFiles('You need to set or specify the input files.')
            
        #register opereation tools
        self.tools = OperationTools()

        #allow rules to be generated automatically or not
        self.automatically_generate_missing_rules = False
        if 'automatically_generate_missing_rules' in kwargs:
            do_auto = bool(kwargs['automatically_generate_missing_rules'])
            self.logger.info(f"Setting automatic rule generation to '{do_auto}'")
            self.automatically_generate_missing_rules = do_auto

        #define a person_id masker, if the person_id are to be masked
        self.person_id_masker = None

        #stores the final pandas dataframe for each CDM object
        # {
        #   'person':pandas.DataFrame,
        #   'measurement':pandas.DataFrame.
        #   ....
        #}
        self.__df_map = {}

        #stores the invididual objects associated to this model
        # {
        #     'observation':
        #     {
        #         'observation_0': <coconnect.cdm.objects.observation.Observation object 0x000>,
        #         'observation_1': <coconnect.cdm.objects.observation.Observation object 0x001>,
        #     },
        #     'measurement':
        #     {
        #         'measurement_0': <coconnect.cdm.objects.measurement.Measurement object 0x000>,
        #         ...
        #     }
        #     ...
        # }
        self.__objects = {}


    def __getitem__(self,key):
        """
        Ability lookup processed objects from the CDM
        Example:
            cdm = CommonDataModel()
            ...
            cdm.process()
            ...
            person = cdm['person']
        Args:
            key (str): The name of the cdm table to be returned
        Returns:
            pandas.DataFrame if a processed object is found, otherwise returns None
        """
        if key not in self.__df_map.keys():
            return None
        else:
            return self.__df_map[key]

    def __setitem__(self,key,obj):
        self.__df_map[key] = obj
    
        
    def add(self,obj):
        if obj._type not in self.__objects:
            self.__objects[obj._type] = {}
            
        if obj.name in self.__objects[obj._type].keys():
            raise Exception(f"Object called {obj.name} already exists")

        self.__objects[obj._type][obj.name] = obj
        self.logger.info(f"Added {obj.name} of type {obj._type}")
        
    def get_objects(self,destination_table):
        self.logger.debug(f"looking for {destination_table}")
        if destination_table not in self.__objects.keys():
            self.logger.error(f"Trying to obtain the table '{destination_table}', but cannot find any objects")
            raise Exception("Something wrong!")

        return [
            obj
            for obj in self.__objects[destination_table].values()
        ]

    
    def mask_person_id(self,df):
        if 'person_id' in df.columns:
            #if masker has not been defined, define it
            if self.person_id_masker is None:
                self.person_id_masker = {
                    x:i+1
                    for i,x in enumerate(df['person_id'].unique())
                }
            #apply the masking
            df['person_id'] = df['person_id'].map(self.person_id_masker)
            self.logger.info(f"Just masked person_id")
        return df
        
    def process(self,output_folder='output_data/'):

        execution_order = sorted(self.__objects.keys(), key=lambda x: x != 'person')

        for destination_table in execution_order:
            self[destination_table] = self.process_table(destination_table)
            self.logger.info(f'finalised {destination_table}')

        if not self.output_folder is None:
            output_folder = self.output_folder
        self.save_to_file(output_folder)
        
    def process_table(self,destination_table):
        objects = self.get_objects(destination_table)
        nobjects = len(objects)
        extra = ""
        if nobjects>1:
            extra="s"
        self.logger.info(f"for {destination_table}: found {nobjects} object{extra}")
        
        if len(objects) == 0:
            return
        
        #execute them all
        dfs = []
        self.logger.info(f"working on {destination_table}")
        logs = {}
        for i,obj in enumerate(objects):
            obj.execute(self)
            df = obj.get_df()
            self.logger.info(f"finished {obj.name} "
                             f"... {i}/{len(objects)}, {len(df)} rows") 
            if len(df) == 0:
                self.logger.warning(f".. {i}/{len(objects)}  no outputs were found ")
                continue
            
            dfs.append(df)
            logs[obj.name] = obj._meta

        #merge together
        self.logger.info(f'Merging {len(dfs)} objects for {destination_table}')
        df_destination = pd.concat(dfs,ignore_index=True)
        #df_destination = self.mask_person_id(df_destination)

        primary_column = df_destination.columns[0]
        if primary_column != 'person_id':
            df_destination[primary_column] = df_destination.reset_index().index + 1
        else:
            df_destination = df_destination.sort_values(primary_column)

        return df_destination


    def save_to_file(self,f_out):
        for name,df in self.__df_map.items():
            if df is None:
                continue
            fname = f'{f_out}/{name}.csv'
            if not os.path.exists(f'{f_out}'):
                self.logger.info(f'making output folder {f_out}')
                os.makedirs(f'{f_out}')
            self.logger.info(f'saving {name} to {fname}')
            df.set_index(df.columns[0],inplace=True)
            df.to_csv(fname,index=True)
            self.logger.info(df.dropna(axis=1,how='all'))
        

    def set_indexing(self,index_map,strict_check=False):
        if self.inputs == None:
            raise NoInputFiles('Trying to indexing before any inputs have been setup')
        
        for key,index in index_map.items():
            if key not in self.inputs:
                self.logger.warning(f"trying to set index '{index}' for '{key}' but this has not been loaded as an inputs!")
                continue

            if index not in self.inputs[key].columns:
                self.logger.error(f"trying to set index '{index}' on dataset '{key}', but this index is not in the columns! something really wrong!")
                continue
                
            self.inputs[key].index = self.inputs[key][index].rename('index') 
