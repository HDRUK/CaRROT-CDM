import os
import pandas as pd
import numpy as np
import json
import copy
import getpass
from time import gmtime, strftime

from .operations import OperationTools
from coconnect.tools.logger import Logger
from coconnect.tools.profiling import Profiler
from coconnect.tools.file_helpers import InputData

from coconnect import __version__ as cc_version
from .objects import DestinationTable

class NoInputFiles(Exception):
    pass


class CommonDataModel:
    """Pythonic Version of the OHDSI CDM.

    This class controls and manages CDM Table objects that are added to it

    When self.process() is executed by the user, all added objects are defined, merged, formatted and finalised, before being dumped to an output file (.tsv file by default).

    """

    def __init__(self, name=None, output_folder="output_data{os.path.sep}",
                 inputs=None, use_profiler=False,
                 automatically_generate_missing_rules=False):
        """
        CommonDataModel class initialisation 
        Args:
            name (str): Give a name for the class to appear in the logging
            output_folder (str): Path of where the output tsv/csv files should be written to.
                                 The default is to save to a folder in the current directory
                                 called 'output_data'.
            inputs (dict or InputData): Input Data can be a dictionary mapping file names to pandas dataframes,
                                        or can be an InputData object
            use_profiler (bool): Turn on/off profiling of the CPU/Memory of running the current process. 
                                 The default is set to false.
        """
        name = self.__class__.__name__ if name is None else name
            
        self.logger = Logger(name)
        self.logger.info(f"CommonDataModel created with version {cc_version}")

        self.output_folder = output_folder
        
        self.profiler = None
        if use_profiler:
            self.profiler = Profiler(name=name)
            self.profiler.start()

        #default separator for output files is a tab
        self._outfile_separator = '\t'

        #perform some checks on the input data
        if isinstance(inputs,dict):
            self.logger.info("Running with an InputData object")
        elif isinstance(inputs,InputData):
            self.logger.info("Running with an InputData object")
        elif self.inputs is None or inputs is None: 
            self.logger.error(inputs)
            raise NoInputFiles("setting up inputs that are not valid!")
            
        if not self.inputs is None:
            self.logger.waring("overwriting inputs")

        self.inputs = inputs
            
        #register opereation tools
        self.tools = OperationTools()

        #allow rules to be generated automatically or not
        self.automatically_generate_missing_rules = automatically_generate_missing_rules
        if self.automatically_generate_missing_rules:
            self.logger.info(f"Turning on automatic rule generation")

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

        #check if objects have already been registered with this class
        #via the decorator methods
        registered_objects = [
            getattr(self,name)
            for name in dir(self)
            if isinstance(getattr(self,name),DestinationTable)
        ]
        #if they have, then include them in this model
        for obj in registered_objects:
            self.add(obj)
        
        #bookkeep some logs
        self.logs = {
            'meta':{
                'version': cc_version,
                'created_by': getpass.getuser(),
                'created_at': strftime("%Y-%m-%dT%H%M%S", gmtime()),
                'dataset':name,
                'output_folder':os.path.abspath(self.output_folder)
            }
        }

    def __del__(self):
        if self.profiler:
            self.profiler.stop()
            df_profile = self.profiler.get_df()
            f_out = self.output_folder
            f_out = f'{f_out}{os.path.sep}logs{os.path.sep}'
            if not os.path.exists(f'{f_out}'):
                self.logger.info(f'making output folder {f_out}')
                os.makedirs(f'{f_out}')
                
            date = self.logs['meta']['created_at']
            fname = f'{f_out}{os.path.sep}statistics_{date}.csv'
            df_profile.to_csv(fname)
            self.logger.info(f"Writen the memory/cpu statistics to {fname}")
            self.logger.info("Finished")

        
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

    def count_objects(self):
        count_map = json.dumps({
            key: len(obj.keys())
            for key,obj in self.__objects.items()
        },indent=6)
        self.logger.info(f"Number of objects to process for each table...\n{count_map}")

    def keys(self):
        return self.__df_map.keys()

    def objects(self):
        return self.__objects

    def process_chunked_data(self):
        i=0
        while True:
            for destination_table in self.execution_order:
                self[destination_table] = self.process_table(destination_table)
                self.logger.info(f'finalised {destination_table} on iteration {i}')
                
            mode = 'w'
            if i>0:
                mode='a'
                
            self.save_to_file(mode=mode)
            self.save_logs(extra=f'_slice_{i}')
            i+=1
            
            try:
                self.inputs.next()
            except StopIteration:
                break

    def process_flat_data(self):
        for destination_table in self.execution_order:
            self[destination_table] = self.process_table(destination_table)
            self.logger.info(f'finalised {destination_table}')

        self.save_to_file()
        self.save_logs()
                
    def process(self):
        #determine the order to execute tables in
        #only thing that matters is to execute the person table first
        # - this is if we want to mask the person_ids and need to save a record of
        #   the link between the unmasked and masked
        self.execution_order = sorted(self.__objects.keys(), key=lambda x: x != 'person')
        self.logger.info(f"Starting processing in order: {self.execution_order}")
        self.count_objects()

        #switch to process the data in chunks or not
        if isinstance(self.inputs,InputData):
            self.process_chunked_data()
        else:
            self.process_flat_data()
            
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
            df = obj.get_df(force_rebuild=False)
            self.logger.info(f"finished {obj.name} "
                             f"... {i}/{len(objects)}, {len(df)} rows") 
            if len(df) == 0:
                self.logger.warning(f".. {i}/{len(objects)}  no outputs were found ")
                continue
            
            dfs.append(df)
            logs[obj.name] = obj._meta

        if len(dfs) == 0:
            return None
            
        #merge together
        self.logger.info(f'Merging {len(dfs)} objects for {destination_table}')
        if len(dfs) == 1:
            df_destination = dfs[0]
        else:
            df_destination = pd.concat(dfs,ignore_index=True)

        #! this section of code may need some work ...
        #person_id masking turned off... assume we dont need this (?)
        #df_destination = self.mask_person_id(df_destination)

        #get the primary columnn
        #this will be <table_name>_id: person_id, observation_id, measurement_id...
        primary_column = df_destination.columns[0]
        #if it's not the person_id
        if primary_column != 'person_id':
            #create an index from 1-N 
            df_destination[primary_column] = df_destination.reset_index().index + 1
        else:
            #otherwise if it's the person_id, sort the values based on this
            df_destination = df_destination.sort_values(primary_column)

        #book the metadata logs
        self.logs[destination_table] = logs
        
            
        #return the finalised full dataframe for this table
        return df_destination

    def save_logs(self,f_out=None,extra=""):
        if f_out == None:
            f_out = self.output_folder
        f_out = f'{f_out}/logs/'
        if not os.path.exists(f'{f_out}'):
            self.logger.info(f'making output folder {f_out}')
            os.makedirs(f'{f_out}')

        date = self.logs['meta']['created_at']
        fname = f'{f_out}/{date}{extra}.json'
        json.dump(self.logs,open(fname,'w'),indent=6)
        self.logger.info(f'saved a log file to {fname}')


    def get_outfile_extension(self):
        if self._outfile_separator == ',':
            return 'csv'
        elif self._outfile_separator == '\t':
            return 'tsv'
        else:
            self.logger.warning(f"Don't know what to do with the extension '{self._outfile_separator}' ")
            self.logger.warning("Defaulting to csv")
            return 'csv'
        
    def save_to_file(self,f_out=None,mode='w'):
        if f_out == None:
            f_out = self.output_folder
        header=True
        if mode == 'a':
            header = False

        file_extension = self.get_outfile_extension()
        
        for name,df in self.__df_map.items():
            if df is None:
                continue
            fname = f'{f_out}/{name}.{file_extension}'
            if not os.path.exists(f'{f_out}'):
                self.logger.info(f'making output folder {f_out}')
                os.makedirs(f'{f_out}')
            if mode == 'w':
                self.logger.info(f'saving {name} to {fname}')
            else:
                self.logger.info(f'updating {name} in {fname}')
            df.set_index(df.columns[0],inplace=True)
            df.to_csv(fname,mode=mode,header=header,index=True,sep=self._outfile_separator)
            self.logger.debug(df.dropna(axis=1,how='all'))

        self.logger.info("finished save to file")

    def set_outfile_separator(self,sep):
        self._outfile_separator = sep
        
    def set_indexing(self,index_map,strict_check=False):
        if self.inputs == None:
            raise NoInputFiles('Trying to indexing before any inputs have been setup')

        for key,index in index_map.items():
            if key not in self.inputs.keys():
                self.logger.warning(f"trying to set index '{index}' for '{key}' but this has not been loaded as an inputs!")
                continue

            if index not in self.inputs[key].columns:
                self.logger.error(f"trying to set index '{index}' on dataset '{key}', but this index is not in the columns! something really wrong!")
                continue
            self.inputs[key].index = self.inputs[key][index].rename('index') 

