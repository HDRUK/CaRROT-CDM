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
from .objects import DestinationTable, FormatterLevel
from .objects import get_cdm_class, get_cdm_decorator
from .decorators import load_file

class NoInputFiles(Exception):
    pass

class PersonExists(Exception):
    pass

class CommonDataModel:
    """Pythonic Version of the OHDSI CDM.

    This class controls and manages CDM Table objects that are added to it

    When self.process() is executed by the user, all added objects are defined, merged, formatted and finalised, before being dumped to an output file (.tsv file by default).

    """

    @classmethod
    def load(cls,**kwargs):
        cdm = cls(**kwargs)
        inputs = kwargs['inputs']
        for fname in inputs.keys():
            destination_table,_ = os.path.splitext(fname)
            obj = get_cdm_decorator(destination_table)(load_file(fname))
            cdm.add(obj)
        return cdm
    

    def __init__(self, name=None, 
                 output_folder=f"output_data{os.path.sep}",
                 output_database=None,
                 indexing_conf=None,
                 person_id_map=None,
                 save_files=True,
                 inputs=None,
                 use_profiler=False,
                 format_level=None,
                 do_mask_person_id=True,
                 automatically_generate_missing_rules=True):
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
        self.profiler = None
        name = self.__class__.__name__ if name is None else self.__class__.__name__ + "::" + name
            
        self.logger = Logger(name)
        self.logger.info(f"CommonDataModel created with version {cc_version}")

        self.output_folder = output_folder
        self.save_files = save_files

        self.do_mask_person_id = do_mask_person_id
        self.indexing_conf = indexing_conf

        if format_level == None:
            format_level = 1
        try:
            format_level = int(format_level)
        except ValueError:
            self.logger.error(f"You as specifying format_level='{format_level}' -- this should be an integer!")
            raise ValueError("format_level not set as an int ")
        
        self.format_level = FormatterLevel(format_level)
        self.profiler = None

        self.output_database = output_database
        self.psql_engine = None
        if self.output_database is not None:
            self.logger.info(f"Running with the output set to '{self.output_database}'")
            try:
                from sqlalchemy import create_engine
                self.psql_engine = create_engine(self.output_database)
            except Exception as err:
                self.logger.critical(f"Failed to make a connection to {self.output_database}")
                raise(err)
        else:
            self.logger.info(f"Running with the output to be dumped to a folder '{self.output_folder}'")

        if use_profiler:
            self.logger.debug(f"Turning on cpu/memory profiling")
            self.profiler = Profiler(name=name)
            self.profiler.start()

        #default separator for output files is a tab
        self._outfile_separator = '\t'

        #perform some checks on the input data
        if isinstance(inputs,dict):
            self.logger.info("Running with an InputData object")
        elif isinstance(inputs,InputData):
            self.logger.info("Running with an InputData object")

        if inputs is not None:
            if hasattr(self,'inputs'):
                self.logger.warning("overwriting inputs")
            self.inputs = inputs
            
        #register opereation tools
        self.tools = OperationTools()

        #allow rules to be generated automatically or not
        self.automatically_fill_missing_columns = automatically_fill_missing_columns
        if self.automatically_fill_missing_columns:
            self.logger.info(f"Turning on automatic cdm column filling")

        #define a person_id masker, if the person_id are to be masked
        self.person_id_masker = self.get_existing_person_id_masker(person_id_map)

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
                'output_folder':os.path.abspath(self.output_folder),
                'total_data_processed':{}
            }
        }

    def __del__(self):
        """
        Class destructor:
              Stops the profiler from running before deleting self
        """
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

    @classmethod
    def from_existing(cls,**kwargs):
        """
        Initialise the CDM model from existing data in the CDM format
        """
        cdm = cls(**kwargs)
        if 'inputs' not in kwargs:
            raise NoInputFiles("you need to specify some inputs")
        inputs = kwargs['inputs']
        #loop over all input names
        for fname in inputs.keys():
            #obtain the name of the destination table
            #e.g fname='person.tsv' we want 'person'
            destination_table,_ = os.path.splitext(fname)
            #
            obj = get_cdm_decorator(destination_table)(load_file(fname))
            cdm.add(obj)
        return cdm

            
        
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
        """
        Registration of a new dataframe for a new object
        Args:
            key (str) : name of the CDM table (e.g. "person")
            obj (pandas.DataFrame) : dataframe to refer to 
        """
        self.logger.debug(f"creating {obj} for {key}")
        self.__df_map[key] = obj
        
    def add(self,obj):
        """
        Function to add a new CDM table (object) to the current model
        Args:
            obj (DestinationTable) : CDM Table to be registered with the class
        """
        if obj._type not in self.__objects:
            self.__objects[obj._type] = {}
            
        if obj.name in self.__objects[obj._type].keys():
            raise Exception(f"Object called {obj.name} already exists")

        self.__objects[obj._type][obj.name] = obj
        self.logger.info(f"Added {obj.name} of type {obj._type}")

    def get_start_index(self,destination_table):
        self.logger.debug(f'getting start index for {destination_table}')

        if self.indexing_conf == None or not self.indexing_conf :
            self.logger.debug(f"no indexing specified, so starting the index for {destination_table} from 1")
            return 1

        if destination_table in self.indexing_conf:
            return int(self.indexing_conf[destination_table])
        else:
            self.logger.warning(self.indexing_conf)
            self.logger.warning("indexing configuration has be parsed "
                                f"but this table ({destination_table}) "
                                "has not be passed, so starting from 1")
            return 1

    def get_existing_person_id_masker(self,fname):
        if fname == None:
            return fname
        elif os.path.exists(fname):
            #this needs to be scalable for large number of person ids
            _df = pd.read_csv(fname,sep='\t').set_index('TARGET_SUBJECT')['SOURCE_SUBJECT']
            return _df.to_dict()
        else:
            self.logger.error(f"Supplied the file {fname} as a file containing already masked person_ids "
                              "file does not exist!!")
            return None
            
    def get_objects(self,destination_table):
        """
        For a given destination table:
        * Retrieve all associated objects that have been registered with the class

        Args:
            destination_table (str) : name of a destination (CDM) table e.g. "person"
        Returns:
            list : a list of destination table objects 
                   e.g. [<person_0>, <person_1>] 
                   which would be objects for male and female mapping
        """
        self.logger.debug(f"looking for {destination_table}")
        if destination_table not in self.__objects.keys():
            self.logger.error(f"Trying to obtain the table '{destination_table}', but cannot find any objects")
            raise Exception("Something wrong!")

        return [
            obj
            for obj in self.__objects[destination_table].values()
        ]

    
    def mask_person_id(self,df,destination_table):
        """
        Given a dataframe object, apply a masking map on the person_id, if one has been created
        Args:
            df (pandas.Dataframe) : input pandas dataframe
        Returns:
            pandas.Dataframe: modified dataframe with person id masked
        """

        if 'person_id' in df.columns:
            #if masker has not been defined, define it
                                                 
            if destination_table == 'person':
                if self.person_id_masker is not None:
                    start_index = int(list(self.person_id_masker.values())[-1]) + 1
                else:
                    self.person_id_masker = {}
                    start_index = self.get_start_index(destination_table)

                for i,x in enumerate(df['person_id'].unique()):
                    index = i+start_index
                    if x in self.person_id_masker:
                        existing_index = self.person_id_masker[x]
                        self.logger.error(f"'{x}' already found in the person_id_masker")
                        self.logger.error(f"'{existing_index}' assigned to this already")
                        self.logger.error(f"was trying to set '{index}'")
                        self.logger.error(f"Most likely cause is this is duplicate data!")
                        raise PersonExists('Duplicate person found!')
                    self.person_id_masker[x] = index
                                   
                os.makedirs(self.output_folder,exist_ok=True)
                dfp = pd.DataFrame.from_dict(self.person_id_masker,orient='index',columns=['SOURCE_SUBJECT'])
                dfp.index.name = 'TARGET_SUBJECT'
                dfp = dfp.reset_index().set_index('SOURCE_SUBJECT')
                file_extension = self.get_outfile_extension()
                fname = f"{self.output_folder}{os.path.sep}global_ids.{file_extension}"
                header = True
                mode = 'w'
                if start_index > self.get_start_index(destination_table):
                    header = False
                    mode = 'a'
                        
                dfp.to_csv(fname,header=header,mode=mode,sep=self._outfile_separator)
            
            
            #apply the masking
            if self.person_id_masker is None:
                raise Exception(f"Person ID masking cannot be performed on"
                                f" {destination_table} as no masker based on a person table has been defined!")

            nbefore = len(df['person_id'])
            df['person_id'] = df['person_id'].map(self.person_id_masker)
            self.logger.info(f"Just masked person_id using integers")
            if destination_table != 'person':
                df.dropna(subset=['person_id'],inplace=True) 
                nafter = len(df['person_id'])
                ndiff = nbefore - nafter
                #if rows have been removed
                if ndiff>0:
                    self.logger.error("There are person_ids in this table that are not in the output person table!")
                    self.logger.error("Either they are not in the original data, or while creating the person table, ")
                    self.logger.error("studies have been removed due to lack of required fields, such as birthdate.")
                    self.logger.error(f"{nafter}/{nbefore} were good, {ndiff} studies are removed.")
            
            
        return df

    def count_objects(self):
        """
        For each CDM (destination) table, count the number of objects associated
        e.g.
        {
           "observation": 6,
           "condition_occurrence": 1,
           "person": 2
        }
        """
        count_map = json.dumps({
            key: len(obj.keys())
            for key,obj in self.__objects.items()
        },indent=6)
        self.logger.info(f"Number of objects to process for each table...\n{count_map}")

    def keys(self):
        """
        For cdm.keys(), return the keys of which objects have been mapped.
        Hence which CDM table dataframes have been created.
        This should be used AFTER cdm.process() has been run, which creates the dataframes.
        """
        return self.__df_map.keys()

    def objects(self):
        """
        Method to retrieve the input objects to the CDM 
        """
        return self.__objects

    def process(self):
        """
        Main functionality of the CommonDataModel class
        When executed, this function determines the order in which to process the CDM tables
        Then determines whether to process chunked or flat data
        """
        #determine the order to execute tables in
        #only thing that matters is to execute the person table first
        # - this is if we want to mask the person_ids and need to save a record of
        #   the link between the unmasked and masked
        self.execution_order = sorted(self.__objects.keys(), key=lambda x: x != 'person')
        self.logger.info(f"Starting processing in order: {self.execution_order}")
        self.count_objects()
        
        #switch to process the data in chunks or not
        if isinstance(self.inputs,InputData):
            if self.inputs.chunksize == None:
                self.process_flat_data()
            else:
                self.process_chunked_data()
        else:
            self.process_flat_data()

    
    def process_chunked_data(self):
        """
        Process chunked data, processes as follows
        * While the chunking of is not yet finished
        * Loop over all CDM tables (via execution order) 
          and process the table (see process_table), returning a dataframe
        * Register the retrieve dataframe with the model  
        * For the current chunk slice, save the data/logs to files
        * Retrieve the next chunk of data

        """

        i=0
        while True:
        
            for destination_table in self.execution_order:
                self[destination_table] = self.process_table(destination_table)
                if not self[destination_table] is None:
                    nrows = len(self[destination_table])
                    self.logger.info(f'finalised {destination_table} on iteration {i} producing {nrows}')

            mode = 'w'
            if i>0:
                mode='a'

            if self.save_files:
                self.save_dataframes(mode=mode)
                self.save_logs(extra=f'_slice_{i}')

            i+=1
            
            try:
                self.inputs.next()
            except StopIteration:
                break

    def process_flat_data(self):
        """
        For processing of flat data (not chunked):
        * Loop over each destination table (via execution order):
        * Process the CDM table (see process_table) retrieving a dataframe
        * Register the retrieve dataframe with the model
        * Save files and logs
        """
        for destination_table in self.execution_order:
            self[destination_table] = self.process_table(destination_table)
            self.logger.info(f'finalised {destination_table}')

        if self.save_files:
            self.save_dataframes()
            self.save_logs()
                
            
    def process_table(self,destination_table):
        """
        Process a CDM (destination) table. The method proceeds as follows:
        * Given a destination table name e.g. 'person' 
        * Retrieve all objects belonging to the given CDM table (e.g. <person_0, person_1>)
        * Loop over each object
        * Retrieve a dataframe for that object given it's definition/rules (see get_df)
        * Concatenate all retrieve dataframes together by stacking on top of each other vertically
        * Create new indexes for the primary column so the go from 1-N 
        
        Args:
            destination_table (str) : name of a destination table to process (e.g. 'person')
        Returns:
            pandas.Dataframe: a merged output dataframe in the CDM format for the destination table
        """
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
        logs = {'objects':{}}
        for i,obj in enumerate(objects):
            obj.execute(self)
            df = obj.get_df(force_rebuild=False)
            self.logger.info(f"finished {obj.name} "
                             f"... {i}/{len(objects)}, {len(df)} rows") 
            if len(df) == 0:
                self.logger.warning(f".. {i}/{len(objects)}  no outputs were found ")
                continue
            
            dfs.append(df)
            logs['objects'][obj.name] = obj._meta

        if len(dfs) == 0:
            return None
            
        #merge together
        self.logger.info(f'Merging {len(dfs)} objects for {destination_table}')
        if len(dfs) == 1:
            df_destination = dfs[0]
        else:
            df_destination = pd.concat(dfs,ignore_index=True)

        #register the total length of the output dataframe
        logs['ntotal'] = len(df_destination)

        #mask the person id
        if self.do_mask_person_id:
            df_destination = self.mask_person_id(df_destination,destination_table)

        #get the primary columnn
        #this will be <table_name>_id: person_id, observation_id, measurement_id...
        primary_column = df_destination.columns[0]
        #if it's not the person_id
        if primary_column != 'person_id':
            #create an index from 1-N
            start_index = self.get_start_index(destination_table)
            #if we're processing chunked data, and nrows have already been created (processed)
            #start the index from this number
            total_data_processed = self.logs['meta']['total_data_processed']
            if destination_table in total_data_processed:
                nrows_processed_so_far = total_data_processed[destination_table]
                start_index += nrows_processed_so_far
                
            df_destination[primary_column] = df_destination.reset_index().index + start_index
        else:
            #otherwise if it's the person_id, sort the values based on this
            df_destination = df_destination.sort_values(primary_column)

        #book the metadata logs
        self.logs[destination_table] = logs

        if destination_table not in self.logs['meta']['total_data_processed']:
            self.logs['meta']['total_data_processed'][destination_table] = 0
        self.logs['meta']['total_data_processed'][destination_table] += len(df_destination)        
        
        #return the finalised full dataframe for this table
        return df_destination

    def save_logs(self,f_out=None,extra=""):
        """
        CommonDataModel keeps logs of various information about what rows have been processed/created/deleted
        these logs are saved to json files by this function.

        Args:
            f_out (str): Name of the output folder to use. Defaults to None and is overwritten as self.output_folder.
            extra (str): Extra string to append to the name of the log file, useful for sliced data.
        """
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
        """
        Work out what the extension of the output file for the dataframes should be.

        Given the '_outfile_separator' to be used in `df.to_csv`,
        work out the file extension.

        At current, only tab separated and comma separated values (files) are supported
        
        Returns:
           str: outfile extension name
        
        """
        if self._outfile_separator == ',':
            return 'csv'
        elif self._outfile_separator == '\t':
            return 'tsv'
        else:
            self.logger.warning(f"Don't know what to do with the extension '{self._outfile_separator}' ")
            self.logger.warning("Defaulting to csv")
            return 'csv'
        

    def save_dataframes(self,mode='w'):
        if self.psql_engine is not None:
            self.save_to_psql(mode='a')
        else:
            self.save_to_file(mode=mode)

    def save_to_psql(self,mode='a'):
        #creat an inspector based on the psql engine
        from sqlalchemy import inspect
        insp  = inspect(self.psql_engine)
        #get the names of existing tables
        existing_tables = insp.get_table_names()

        #set the method of pandas based on the mode supplied
        if mode == 'a':
            if_exists = 'append'
        elif mode == 'r':
            if_exists = 'replace'
        elif mode == 'w':
            if_exists = 'fail'
        else:
            raise Exception(f"Unknown mode for dumping to psql, mode = '{mode}'")

        #loop over all created dataframes in the CDM
        for name,df in self.__df_map.items():
            #skip dead dataframes
            if df is None:
                continue

            #check if the table exists already
            table_exists = name in existing_tables

            #index the dataframe
            pk = df.columns[0]
            df.set_index(pk,inplace=True)
            self.logger.info(f'updating {name} in {self.psql_engine}')

            #check if the table already exists in the psql database
            if table_exists:
                #get the last row
                last_row_existing = pd.read_sql(f"select {pk} from {name} "
                                                f"order by {pk} desc limit 1",
                                                self.psql_engine)
                
                #if there's already a row and the mode is set to append
                if len(last_row_existing) > 0 and mode == 'a':
                    #get the cell value of the (this will be the id, e.g. condition_occurrence_id)
                    last_pk_existing = last_row_existing.iloc[0,0]
                    #get the index integer of this current dataframe
                    first_pk_new = df.index[0]
                    #workout and increase the indexing so the indexes are new
                    index_diff = last_pk_existing - first_pk_new
                    if index_diff >= 0:
                        self.logger.info("increasing index as already exists in psql")
                        df.index += index_diff + 1

            #dump to sql
            df.to_sql(name, self.psql_engine,if_exists=if_exists) 

        self.logger.info("finished save to psql")

    def save_to_file(self,f_out=None,mode='w'):
        """
        Save the dataframe processed by the CommonDataModel to files.

        Args:
            f_out (str): Name of the output folder to use. Defaults to None and is overwritten as self.output_folder.
            mode (str): Mode for how to write the file. Append or write. Default is 'w' or write mode.
        
        """
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

            for col in df.columns:
                if col.endswith("_id"):
                    df[col] = df[col].astype(float).astype(pd.Int64Dtype())
                    
            df.set_index(df.columns[0],inplace=True)
            self.logger.debug(df.dtypes)
            df.to_csv(fname,mode=mode,header=header,index=True,sep=self._outfile_separator)

            if 'output_files' not in self.logs['meta']:
                self.logs['meta']['output_files'] = {}

            self.logs['meta']['output_files'][name] = fname
            self.logger.debug(df.dropna(axis=1,how='all'))

        self.logger.info("finished save to file")

    def set_outfile_separator(self,sep):
        """
        Set which separator to use, e.g. ',' or '\t' 

        Args:
            sep (str): which separator to use when writing csv (tsv) files
        """
        self._outfile_separator = sep
        
    def set_indexing(self,index_map,strict_check=False):
        """
        Create indexes on input files which would allow rules to use data from 
        different input tables.

        Args:
            index_map (dict): a map between the filename and what should be the column used for indexing 

        """
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

