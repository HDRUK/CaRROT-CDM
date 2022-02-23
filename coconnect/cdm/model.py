import os
import pandas as pd
import numpy as np
import json
import copy
import getpass

import shutil
import threading
import concurrent.futures
from time import gmtime, strftime, sleep, time

from .operations import OperationTools
from coconnect.tools.logger import Logger
from coconnect.tools.profiling import Profiler
from coconnect.io import DataCollection

from coconnect import __version__ as cc_version
from .objects import DestinationTable, FormatterLevel
from .objects import get_cdm_class, get_cdm_decorator
from .decorators import load_file, analysis

class BadInputObject(Exception):
    pass

class PersonExists(Exception):
    pass


class CommonDataModel(Logger):
    """Pythonic Version of the OHDSI CDM.

    This class controls and manages CDM Table objects that are added to it

    When self.process() is executed by the user, all added objects are defined, merged, formatted and finalised, before being dumped to an output file (.tsv file by default).

    """

    
    @classmethod
    def load(cls,inputs,**kwargs):
        default_kwargs = {'save_files':False,'do_mask_person_id':False,'format_level':0}
        default_kwargs.update(kwargs)
        cdm = cls(**default_kwargs)
        cdm._load_inputs(inputs)
        return cdm
    

    def __init__(self, name=None, omop_version='5.3.1',
                 #output_folder=f"output_data{os.path.sep}",
                 #output_database=None,
                 outputs = None,
                 #outfile_separator='\t',
                 indexing_conf=None,
                 person_id_map=None,
                 save_files=True,
                 save_log_files=False,
                 inputs=None,
                 use_profiler=False,
                 format_level=None,
                 do_mask_person_id=True,
                 automatically_fill_missing_columns=True):
        """
        CommonDataModel class initialisation 
        Args:
            name (str): Give a name for the class to appear in the logging
            output_folder (str): Path of where the output tsv/csv files should be written to.
                                 The default is to save to a folder in the current directory
                                 called 'output_data'.
            inputs (dict or DataCollection): inputs can be a dictionary mapping file names to pandas dataframes,
                                        or can be a DataCollection object
            use_profiler (bool): Turn on/off profiling of the CPU/Memory of running the current process. 
                                 The default is set to false.
        """
        self.profiler = None
        name = self.__class__.__name__ if name is None else self.__class__.__name__ + "::" + name
            
        self.logger.info(f"CommonDataModel ({omop_version}) created with co-connect-tools version {cc_version}")

        self.omop_version = omop_version

        # remove....?
        self.save_files = save_files
        self.save_log_files = save_log_files

        self.do_mask_person_id = do_mask_person_id
        self.indexing_conf = indexing_conf
        self.execution_order = None
        
        if format_level == None:
            format_level = 1
        try:
            format_level = int(format_level)
        except ValueError:
            self.logger.error(f"You as specifying format_level='{format_level}' -- this should be an integer!")
            raise ValueError("format_level not set as an int ")
        
        self.format_level = FormatterLevel(format_level)
        self.profiler = None

        self.outputs = outputs
        # self.output_folder = output_folder
        # self.output_database = output_database
        # self.psql_engine = None
        # if self.output_database is not None:
        #     self.logger.info(f"Running with the output set to '{self.output_database}'")
        #     try:
        #     except Exception as err:
        #         self.logger.critical(f"Failed to make a connection to {self.output_database}")
        #         raise(err)
        # else:
        #     self.logger.info(f"Running with the output to be dumped to a folder '{self.output_folder}'")

        if use_profiler:
            self.logger.debug(f"Turning on cpu/memory profiling")
            self.profiler = Profiler(name=name)
            self.profiler.start()

        #default separator for output files is a tab
        #self._outfile_separator = outfile_separator

        #perform some checks on the input data
        if isinstance(inputs,dict):
            self.logger.info("Running with an DataCollection object")
        elif isinstance(inputs,DataCollection):
            self.logger.info("Running with an DataCollection object")
        elif inputs is not None:
            _type = type(inputs).__name__
            raise BadInputObject(f"input object {inputs} is of type {_type}, which is not a valid input object")

        if inputs is not None:
            if hasattr(self,'inputs'):
                self.logger.warning("overwriting inputs")
            self.inputs = inputs
        elif not hasattr(self,'inputs'):
            self.inputs = None
            
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
            obj.inputs = self.inputs
            self.add(obj)

        self.__analyses = {
            name:getattr(self,name)
            for name in dir(self)
            if isinstance(getattr(self,name),analysis)
        }
                    
        #bookkeep some logs
        self.logs = {
            'meta':{
                'version': cc_version,
                'created_by': getpass.getuser(),
                'created_at': strftime("%Y-%m-%dT%H%M%S", gmtime()),
                'dataset':name,
                #'output_folder':os.path.abspath(self.output_folder),
                'total_data_processed':{}
            }
        }

    def _load_inputs(self,inputs):
        for fname in inputs.keys():
            destination_table,_ = os.path.splitext(fname)
            try:
                obj = get_cdm_class(destination_table).from_df(inputs[fname],destination_table)
            except KeyError:
                self.logger.warning(f"Not loading {fname}, this is not a valid CDM Table")
                continue
                
            df = obj.get_df(force_rebuild=False)
            self[destination_table] = obj

        
    def close(self):
        """
        Class destructor:
              Stops the profiler from running before deleting self
        """

        
        if not hasattr(self,'profiler'):
            return
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

        obj.cdm = self
        obj.format_level = self.format_level
        
        self.__objects[obj._type][obj.name] = obj
        self.logger.info(f"Added {obj.name} of type {obj._type}")


    def add_analysis(self,func,_id=None):
        if _id is None:
            _id = hex(id(func))
        self.__analyses[_id] = func

    def get_analyses(self):
        return self.__analyses
    def get_analysis(self,key):
        return self.__analyses[key]
        
    def run_analysis(self,f):
        return f(self)
        
    def run_analyses(self,analyses=None,max_workers=4):
            
        def msg(x):
            self.logger.info(f"finished with {x}")
            self.logger.info(x.result())

        start = time()
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for name,f in self.__analyses.items():
                self.logger.info(f"Start thread for {f} ")
                future = executor.submit(f, self)
                future.add_done_callback(msg)
                futures[future] = name

            while True:
                status = {futures[f]:{'status':'running' if f.running() else 'done' if f.done() else 'waiting'} for f in futures}
                self.logger.debug(json.dumps(status,indent=6))
                if all([f.done() for f in futures]):
                    break
                sleep(1)
                
                
        results = {}
        for future in concurrent.futures.as_completed(futures):
            _id = futures[future]
            results[_id] = future.result()

        end = time() - start
        self.logger.info(f"Running analyses took {end} seconds")
        return results

    def find_one(self,config,cols=None,dropna=False):
        return self.filter(config,cols,dropna).sample(frac=1).iloc[0]
        
    def find(self,config,cols=None,dropna=False):
        return self.filter(config,cols,dropna)

    def filter(self,config,cols=None,dropna=False):
        retval = None
        for obj in config:
            if isinstance(obj,str):
                df = self[obj].get_df().set_index('person_id')
                if retval is None:
                    retval = df
                else:
                    retval = retval.merge(df,left_index=True,right_index=True)
            elif isinstance(obj,dict):
                for key,value in obj.items():
                    df = self[key].filter(value).set_index('person_id')
                    if retval is None:
                        retval = df
                    else:
                        retval = retval.merge(df,left_index=True,right_index=True)
            else:
                raise NotImplementedError("need to pass a json object to filter()")

        if dropna:
            retval = retval.dropna(axis=1)
        if cols is not None:
            retval = retval[cols]
        return retval
        
    def get_all_objects(self):
        return [ obj for collection in self.__objects.values() for obj in collection.values()]

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
            self.logger.error(f"Supplied the file {fname} as a file containing already masked person_ids ")
            raise FileNotFoundError("{fname} file does not exist!!")


    def clear_objects(self,destination_table=None):
        if destination_table:
            self.__objects[destination_table].clear()
        else:
            for destination_table in self.__objects:
                self.__objects[destination_table].clear()
            
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
        if destination_table == None:
            return self.get_all_objects()
        
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
                    new = False
                else:
                    self.person_id_masker = {}
                    start_index = self.get_start_index(destination_table)
                    new = True

                person_id_masker = {}
                for i,x in enumerate(df['person_id'].unique()):
                    index = i+start_index
                    if x in self.person_id_masker:
                        existing_index = self.person_id_masker[x]
                        self.logger.error(f"'{x}' already found in the person_id_masker")
                        self.logger.error(f"'{existing_index}' assigned to this already")
                        self.logger.error(f"was trying to set '{index}'")
                        self.logger.error(f"Most likely cause is this is duplicate data!")
                        raise PersonExists('Duplicate person found!')
                    person_id_masker[x] = index

                self.person_id_masker.update(person_id_masker)

                if self.outputs:
                    dfp = pd.DataFrame.from_dict(person_id_masker,orient='index',columns=['SOURCE_SUBJECT'])
                    dfp.index.name = 'TARGET_SUBJECT'
                    dfp = dfp.reset_index().set_index('SOURCE_SUBJECT')
                    self.outputs.write("global_ids",dfp)
                        
            #apply the masking
            if self.person_id_masker is None:
                raise Exception(f"Person ID masking cannot be performed on"
                                f" {destination_table} as no masker based on a person table has been defined!")

            nbefore = len(df['person_id'])
            df['person_id'] = df['person_id'].map(self.person_id_masker)

            self.logger.debug(f"Just masked person_id using integers")
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

                    
    def process(self,object_list=None,conserve_memory=False):
        """
        Process chunked data, processes as follows
        * While the chunking of is not yet finished
        * Loop over all CDM tables (via execution order) 
          and process the table (see process_table), returning a dataframe
        * Register the retrieve dataframe with the model  
        * For the current chunk slice, save the data/logs to files
        * Retrieve the next chunk of data

        """
        self.execution_order = self.get_execution_order()
        self.logger.info(f"Starting processing in order: {self.execution_order}")
        self.count_objects()

        for destination_table in self.execution_order:
            first = True
            i = 0
            while True:                
                df_generator = self.process_table(destination_table,object_list=object_list)
                ntables = 0
                nrows = 0
                dfs = []
                for j,obj in enumerate(df_generator):

                    df = obj.get_df()
                    ntables +=1
                    nrows += len(df)
                    if self.save_files:
                        mode = 'w' if first else 'a'
                        self.save_dataframe(destination_table,df,mode=mode)
                        first = False
                    if not conserve_memory:
                        dfs.append(df)
                    else:
                        obj.clear()
                        del df
                        df = None

                if not conserve_memory:
                    self[destination_table] = pd.concat(dfs,ignore_index=True)

                self.logger.info(f'finalised {destination_table} on iteration {i} producing {nrows} rows from {ntables} tables')
                
                #move onto the next iteration                
                i+=1

                if self.inputs:
                    try:
                        self.inputs.next()
                    except StopIteration:
                        break
                else:
                    break

            #if inputs are defined, and we havent just finished the last table,
            #reset the inputs
            if self.inputs and not destination_table == self.execution_order[-1]:
                self.inputs.reset()
                
    def process_simult(self,object_list=None,conserve_memory=False):
        """
        process simulataneously
        """
        self.execution_order = self.get_execution_order()
        self.logger.info(f"Starting processing in order: {self.execution_order}")
        self.count_objects()
        i=0
        while True:
            for destination_table in self.execution_order:
                df_generator = self.process_table(destination_table,object_list=object_list)
                ntables = 0
                nrows = 0
                dfs = []
                for j,obj in enumerate(df_generator):
                    df = obj.get_df()
                    ntables +=1
                    nrows += len(df)
                    if self.save_files:
                        mode = 'w' if i==0 else 'a'
                        self.save_dataframe(destination_table,df,mode=mode)
                        first = False
                    if not conserve_memory:
                        dfs.append(df)
                    else:
                        obj.clear()
                        del df
                        df = None
                if not conserve_memory:
                    self[destination_table] = pd.concat(dfs,ignore_index=True)
                        
            if self.inputs:
                try:
                    self.inputs.next()
                except StopIteration:
                    break
            else:
                break
            i+=1



    def get_tables(self):
        return list(self.__objects.keys())

    def get_execution_order(self):
        if not self.execution_order:
            self.execution_order = sorted(self.__objects.keys(), key=lambda x: x != 'person')
        return self.execution_order
    
    def set_execution_order(self,order):
        self.execution_order = order
            
    def process_table(self,destination_table,object_list=None):
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
            object_list (list) : [optional] list of objects to process
        Returns:
            list(pandas.Dataframe): a dataframes in the CDM format for this destination table
        """
        objects = self.get_objects(destination_table)
        if object_list:
            objects = [obj for obj in object_list if obj in objects]
        
        nobjects = len(objects)
        extra = ""
        if nobjects>1:
            extra="s"
        self.logger.info(f"for {destination_table}: found {nobjects} object{extra}")
        
        if len(objects) == 0:
            yield None
        
        #execute them all
        dfs = []
        self.logger.info(f"working on {destination_table}")
        logs = {'objects':{}}

        if destination_table not in self.logs['meta']['total_data_processed']:
            self.logs['meta']['total_data_processed'][destination_table] = 0

        nrows_processed = self.logs['meta']['total_data_processed'][destination_table]
            
        for i,obj in enumerate(objects):
            self.logger.info(f"starting on {obj.name}")

            start_index = self.get_start_index(destination_table)
            start_index += nrows_processed
            
            df = obj.get_df(force_rebuild=True,start_index=start_index)
            self.logger.info(f"finished {obj.name} ({hex(id(df))}) "
                             f"... {i+1}/{len(objects)} completed, {len(df)} rows") 
            if len(df) == 0:
                self.logger.warning(f".. no outputs were found ")
                continue

            if self.do_mask_person_id:
                df = self.mask_person_id(df,destination_table)
            
            nrows_processed += len(df)
            obj.set_df(df)
            yield obj
            #dfs.append(df)
            #logs['objects'][obj.name] = obj._meta

        

        # self.logs['meta']['total_data_processed'][destination_table] += nrows_processed
        
        # #merge together
        # if len(dfs) == 1:
        #     df_destination = dfs[0]
        # else:
        #     self.logger.info(f'Merging {len(dfs)} objects for {destination_table}')
        #     df_destination = pd.concat(dfs,ignore_index=True)
        
        # df_destination = pd.concat(dfs,ignore_index=True)
        
        
        # if len(dfs) == 0:
        #     self[destination_table] = None
        #     return None
        
            
        # #register the total length of the output dataframe
        # logs['ntotal'] = len(df_destination)

        # #mask the person id
        # if self.do_mask_person_id:
        #     df_destination = self.mask_person_id(df_destination,destination_table)

        # #get the primary columnn
        # #this will be <table_name>_id: person_id, observation_id, measurement_id...
        # primary_column = df_destination.columns[0]
        # #if it's not the person_id
        # is_integer = np.issubdtype(df_destination[primary_column].dtype,np.integer)
        # if primary_column != 'person_id' and is_integer:
        #     #create an index from 1-N
        #     start_index = self.get_start_index(destination_table)
        #     #if we're processing chunked data, and nrows have already been created (processed)
        #     #start the index from this number
        #     total_data_processed = self.logs['meta']['total_data_processed']
        #     if destination_table in total_data_processed:
        #         nrows_processed_so_far = total_data_processed[destination_table]
        #         start_index += nrows_processed_so_far
                
        #     df_destination[primary_column] = df_destination.reset_index().index + start_index
        # elif is_integer:
        #     #otherwise if it's the person_id, sort the values based on this
        #     df_destination = df_destination.sort_values(primary_column)
        # else:
        #     df_destination.reset_index(inplace=True, drop=True)
        #     df_destination.sort_values(primary_column,ignore_index=True,inplace=True)

            
        # #book the metadata logs
        # self.logs[destination_table] = logs

        # if destination_table not in self.logs['meta']['total_data_processed']:
        #     self.logs['meta']['total_data_processed'][destination_table] = 0
        # self.logs['meta']['total_data_processed'][destination_table] += len(df_destination)        
        
        # #finalised full dataframe for this table
        # try:
        #     _class = get_cdm_class(destination_table)
        # except KeyError:
        #     _class = type(objects[0])

        # obj = _class.from_df(df_destination,destination_table)

        # self[destination_table] = obj


    # def save_logs(self,f_out=None,extra=""):
    #     """
    #     CommonDataModel keeps logs of various information about what rows have been processed/created/deleted
    #     these logs are saved to json files by this function.

    #     Args:
    #         f_out (str): Name of the output folder to use. Defaults to None and is overwritten as self.output_folder.
    #         extra (str): Extra string to append to the name of the log file, useful for sliced data.
    #     """
    #     if f_out == None:
    #         f_out = self.output_folder
    #     f_out = f'{f_out}/logs/'
    #     if not os.path.exists(f'{f_out}'):
    #         self.logger.info(f'making output folder {f_out}')
    #         os.makedirs(f'{f_out}')

    #     date = self.logs['meta']['created_at']
    #     fname = f'{f_out}/{date}{extra}.json'
    #     json.dump(self.logs,open(fname,'w'),indent=6)
    #     self.logger.info(f'saved a log file to {fname}')
        

    def save_dataframe(self,table,df=None,mode='w'):
        if self.outputs:
            _id = hex(id(df))
            self.logger.info(f"saving dataframe ({_id}) to {self.outputs}")
            self.outputs.write(table,df,mode)
    
    def set_person_id_map(self,person_id_map):
        self.person_id_masker = self.get_existing_person_id_masker(person_id_map)

        
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

