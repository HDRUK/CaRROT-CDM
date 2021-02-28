"""ETLTool

A program for mapping ETL to CDM based on input datasets, structual mapping and term mapping csv files

Contact: CO-CONNECT@dundee.ac.uk
First Created: 11/02/2021

Example:

    Run the tool::

        $ etl2cdm -v 
       --inputs sample_input_data/patients_sample.csv\
       --structural-mapping sample_input_data/rules1.csv\
       --term-mapping sample_input_data/rules2.csv


Todo:
    * 


"""
import logging
import coloredlogs
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'
import os
import pandas as pd
import numpy as np
import copy
import json
import re
import random


from .operations import ETLOperations
from .exceptions import NoInputData, NoInputData, \
    NoTermMapping, BadStructuralMapping, MadMapping,\
    MissingRequiredMapping, BadDestinationField,\
    BadJoin


class ETLTool:
    """
    A class for the ETLTool runner, this will handle the loading of the input files
    """
    def get_source_table_name(self, fname, truncate_because_excel_is_stupid=True):
        """
        Retrieve source table name from the file name
        - strip the directory name
        - check that the file is a csv file
        - throw and error if it's not a csv file, because we cant handle that yet (ever?)
        - if is a csv file, return the name:
           - if the fname is /blahhh/patients.csv
           - 'patients' will be returned as teh source table name
        """
        fname = fname.split("/")[-1]
            
        self.logger.debug(f'Extracting the name of the table for input: {fname}')
        if fname[-4:] == '.csv':
            #fname = fname.split('.csv')[0] #turn off for now -- revisit, Calum
            if truncate_because_excel_is_stupid:
                fname = fname[:31]

            return fname
        
        raise NotImplementedError(f"{fname} is not a .csv file. Don't know how to handle non csv files yet!")

    def load_df_chunks(self,fname,chunk_size=None):
        """
        Extract a pandas Dataframe from an input csv file
        Args:
           fname (str): the file name
           chunksize(int): specify how many rows to read in per chunk
        Returns: 
           Pandas TextFileReader
        """
        if chunk_size == None:
            chunk_size = self.chunk_size
            
        chunks = pd.read_csv(fname,chunksize=chunk_size)
        return chunks

    def load_df(self,fname,lower_case=True):
        """
        Extract a pandas Dataframe from an input csv file
        Args:
           fname (str): the file name
           lower_case (bool): whether to lower all the names of the columns to be lowercase or not 
        Returns: 
           Pandas DataFrame
        """
        df = pd.read_csv(fname)
        if lower_case:
            df.columns = df.columns.str.lower()
        return df

    def get_structural_mapping_df(self):
        return self.df_structural_mapping

    def get_term_mapping_df(self):
        return self.df_term_mapping

    def get_input_names(self):
        return list(self.map_input_files.keys())

    def get_input_df(self,key,n=10):
        df =  pd.read_csv(self.map_input_files[key])
        return df

    def get_output_df(self,destination_table):
        if self.map_output_data is None:
            self.logger.warning("You're trying to get the output df before running the tool")
            return None
        return self.load_df(self.map_output_data[destination_table])

    def create_logger(self):
        """
        Initialisation of a logging system for cli messages
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        formatter = coloredlogs.ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info('Starting the tool')

    def set_output_folder(self,output_folder):
        self.output_data_folder = output_folder
        if not os.path.exists(self.output_data_folder):
            self.logger.info(f'Creating an output data folder: {self.output_data_folder}')
            os.makedirs(self.output_data_folder)

    def set_perform_person_id_mask(self,b_value):
        self.perform_person_id_mask = b_value

    def set_override_source_term_mapping(self,b_value):
        self.override_source_term_mapping = b_value

    def set_use_auto_functions(self,b_value):
        self.use_auto_functions = b_value
        
    def set_verbose(self,verbose=True):
        """
        Set the message level of the tool to be verbose or not
        Args:
           verbose (bool):  whether to log messages to help debug or not
        """
        self.verbose = verbose
        if self.verbose:
            self.logger.setLevel(logging.DEBUG)

    def set_chunk_size(self,chunk_size):
        """
        Set the chunk size of the number of rows of a table to load into memory in each batch
        Args:
            chunk_size (long int) : the number of rows to process 
        """
        self.chunk_size = chunk_size
        
    def set_max_chunks(self,n):
       """
       """
       self.max_chunks = n
       
    def set_save_files(self,b_save):
        self.save_files = b_save
        
    def set_merge_files(self,b_save):
        self.merge_files = b_save
        
        
    def load_cdm(self,f_cdm):
        """
        Load the default cdm model (v5.3.1) into a pandas dataframe
        Args:
           None
        Returns:
           None
        """
        self.df_cdm = pd.read_csv(f_cdm,encoding="ISO-8859-1").set_index('table')
        self.df_cdm['required'] = self.df_cdm['required'] == 'Yes'

        self.logger.debug(self.df_cdm)


    def check_file(self,fname):
        if not os.path.exists(fname):
            _fname = fname
            fname = f'{self.dir_path}/{fname}'
            if not os.path.exists(fname):
                raise FileNotFoundError(f'Cannot find the file "{_fname}", also tried also looking in "{self.dir_path}" ')
        return fname
            
        
    def load_input_data(self,f_inputs):
        """
        Load the input data, which is a list of input files into pandas dataframes
        and map, based on the original file name, to the dataframe: {name: dataframe}
        Args:
           None
        Returns:
           None
        """

        f_inputs = [self.check_file(fname) for fname in f_inputs]
            
        self.map_input_files = {
            self.get_source_table_name(fname): fname
            for fname in f_inputs
        }
        tables = list(self.map_input_files.keys())
        self.logger.info(f'found the following input tables: {tables}')

    def load_structural_mapping(self,f_structural_mapping):
        """
        Load the structural mapping into a pandas dataframe

        Args:
           None
        Returns:
           None
        """

        #perform a check to see if the file exists
        f_structural_mapping = self.check_file(f_structural_mapping)
        
        #set the index to be a multindex of ['destination_table','rule_id']
        #this is just going to help us out down the line
        #when we use df_structural_mapping.loc[table_name] to easily extract
        #structural mapping rules for associated with the given source table
        
        self.df_structural_mapping = self.load_df(f_structural_mapping)
        
        self.logger.debug(self.df_structural_mapping)
        self.logger.info(f'Loaded the structural mapping with {len(self.df_structural_mapping)} rules')
        
                
    def load_term_mapping(self,f_term_mapping=None, init=False):
        """
        Load the term mapping into a pandas dataframe

        Args:
           None
        Returns:
           None
        """
        if f_term_mapping == None:
            if not init:
                self.logger.warning('No term mapping specified')
            self.df_term_mapping = pd.DataFrame()
            return

        f_term_mapping = self.check_file(f_term_mapping)
        
        self.df_term_mapping = self.load_df(f_term_mapping)\
                                         .set_index(['rule_id'])
        self.logger.debug(self.df_term_mapping)
        self.logger.info(f'Loaded the term mapping with {len(self.df_term_mapping)} rules')


    def save_lookup_table(self,masker,destination_table,source_field,mode='w',header=True):
        """
        Save a dataframe series in a table to a file
        Args:
           None
        Returns:
           None
        """

        outfolder = f'{self.output_data_folder}/masks'
        if not os.path.exists(outfolder):
            self.logger.info(f'Creating a new folder: {outfolder}')
            os.makedirs(outfolder)

        outname = f'{outfolder}/lookup_{destination_table}_{source_field}.csv'
        self.logger.info(f'Writing a lookup dictionary of {source_field} to index')
        self.logger.info(f'Final being saved: {outname}')
        series = pd.DataFrame.from_dict(masker,orient='index',columns=[f'{source_field}'])
        series.to_csv(outname,
                      index_label=f'original_{source_field}',
                      mode=mode,
                      header=header)

        
    def get_destination_tables(self):
        """
        Get the names of all unique names of 'destination_table' that are defined in the df_structural mapping
        Args:
           None
        Returns:
           numpy.ndarray
        """
        return self.df_structural_mapping['destination_table'].unique()
    
    def get_mapped_fields(self,table):
        """
        Get the names all the destination fields that are associated with the structural mapping of a partiular table
        Args:
           table (str): name of a table
        Returns:
           list: list of all destination fields defined in the rules for this cdm object
        """
        return self.df_structural_mapping.set_index('destination_table')\
                                         .loc[[table]]['destination_field'].to_list()

    def get_structural_mapping(self,destination_table,source_table):
        """
        Gets the dataframe for the structural mapping and filtered by the table names
        then sets the index to be the destination field (for later convienience 
        Args:
           destination table (str): name of the CDM table
           source table (str): name of the input dataset tabel
        Returns:
           pandas.DataFrame : a new pandas dataframe with 'destination_field' as the index
        """
                
        return self.df_structural_mapping\
                   .set_index(['destination_table','source_table'])\
                   .sort_index()\
                   .loc[(destination_table,source_table)]\
                   .reset_index().set_index('destination_field')
    
    def get_source_tables(self,table):
        """
        Gets the dataframe for the structural mapping and filtered by the table name
        then looks up all the unique source tables associated with this structural mapping
        there really shouldn't be more than one source table associated to the (destination) table
        Args:
           table (str): name of a (destination) table in the CDM
        Returns:
           list(str): the names of the source tables associated with this mapping
        """
        source_tables = list(self.df_structural_mapping\
                             .set_index('destination_table')\
                             .loc[[table]]['source_table'].unique())

        for i,source_table in enumerate(source_tables):
            if source_table not in self.map_input_files.keys():
                self.logger.warning(f"You have specified a mapping for  \"{source_table}\" but this cannot be found in any of the input datasets: {self.map_input_files.keys()}")
                self.logger.warning("Going to try with lowering the name to lower cases WhiteRabbit are stuck in the 90s")
                _source_table = source_table
                source_table = source_table.lower()
                source_tables[i] = source_table

                if source_table not in self.map_input_files.keys():
                    self.logger.warning(f"You have specified a mapping for  \"{_source_table}\" but this cannot be found in any of the input datasets: {self.map_input_files.keys()}")
                    raise LookupError(f'Cannot find {source_table}!')
                                
        
        return source_tables


    def map_via_rule(self,df,df_map,source_field,destination_field):
        df_orig = df[[source_field]]

        orig_type = df_orig[source_field].dtype
        map_type = df_map['source_term'].dtype

        #check for truncation of terms!
        is_truncation = any(df_map['source_term'].str.contains('List truncated'))
        
        
        #need this step to make sure theyre the same type
        #this isnt working by default because we dumped the csvs with "blah","blah"
        if map_type != orig_type and not is_truncation:
            try:
                df_map['source_term'] = df_map['source_term'].astype(orig_type)
            except ValueError as err:
                orig = df_orig[source_field]

                if len(orig.dropna()) == 0:
                       self.logger.warning('This mapping wont work as the source field ({source_field}) '
                                           'is completely null. '
                                           "I'm giving up on mapping it.")
                       return df_orig
                else:
                       self.logger.error("You're really trying some bizarre mapping here. "
                                         "The types are completely different")
                       self.logger.error(f"Mapping source type = {map_type}")
                       self.logger.error(f"Original source type = {orig_type}")
                       
                       new = df_map['source_term']
                       self.logger.error("EXAMPLEs")
                       self.logger.error(f"Source : {orig}")
                       self.logger.error(f"Trying being mapped with: {new}")
                       
                       raise MadMapping(err)
                
        #this is a temp hack, we should remove this!!!
        if is_truncation:
            new_term = df_map.iloc[0]['destination_term']
            df_orig[df_orig.notnull()] = new_term
            df_orig = df_orig.rename({
                source_field : destination_field
            },axis=1)
            self.logger.warning('Found a truncation, this should be made decrepit soon. '
                                'You would have received a pandas warning about slice copying. '
                                'CBA to fix this as this will be removed')

        else:
            #pandas removes the index when using merge
            #need to preserve it for when we're chunking data
            #https://stackoverflow.com/questions/11976503/how-to-keep-index-when-using-pandas-merge
            _index = df_orig.index
            
            df_orig = df_orig.merge(df_map,
                                left_on=source_field,
                                right_on='source_term',
                                how='left').set_index(_index)

            df_orig = df_orig[['destination_term']].rename(
                {
                    'destination_term':destination_field
                },axis=1)

        df_bad = df_orig.index[df_orig.isnull().any(axis=1)]
        if len(df_bad) > 0 :
            self.logger.warning(f'Found {len(df_bad)}/{len(df_orig)} bad rows.'
                                f' For {source_field} mapped to {destination_field}')
            
            self.logger.warning('These have no specification of how to map them or there are NaN values ')
            
            #n_nan = len(df_temp[df_temp.isnull() == True])
            #self.logger.warning(f'... {n_nan} of these indicies are bad because of NaN values')
            #n_unmapped = len(df_temp[df_temp.isnull() == False])
            #self.logger.warning(f'... {n_unmapped} of these indicies are unmapped')
            
            #add a switch to drop any rows that have a nan value aka the term mapping failed
            if self.aggressive_drop:
                df_orig = df_orig.dropna()

        return df_orig

    def map_auto_extract(self,df,source_field,destination_field):
        """
        Perform auto one-to-one mapping
        Args:
           df (pandas.DataFrame): the dataframe for the original source data
           source_field (str): the name of the field(column) in the source data 
           destination_field (str): the name of the field(column) to be set as the new output
        Returns:
           a new pandas dataframe where the source field has been automatically mapped to the destination field
           by looking up a function to apply to it.
           E.g. if year_of_birth, it'll know to apply EXTRACT YEAR
        """
        if destination_field not in self.allowed_operations.auto_functions:
            self.logger.error("Something really wrong if map_auto_extract() is called on a destination field"
                              "that is not in allowed_operations.auto_functions")
            raise ValueError(f'Cannot find {destination_field} in allowed_operations.auto_functions')
        
        function = self.allowed_operations.auto_functions[destination_field]
        ret = function(df,column=source_field)
        return ret.to_frame(destination_field)
                                    
    
    def map_one_to_one(self,df,source_field,destination_field):
        """
        Perform one-to-one mapping of variables between the source and destination field
        Args:
           df (pandas dataframe): the dataframe for the original source data
           source_field (str): the name of the field(column) in the source data 
           destination_field: the name of the field(column) to be set as the new output
        Returns:
           a new pandas dataframe where the source field has been directly mapped to the destination field
        """

        #make a series which is just the source field
        #convert it to a dataframe and change the name to be the destination field
        return df[source_field].to_frame(destination_field)

    def get_year_from_date(self,df,**kwargs):
        """
        Convert a dataframe containing a datatime into the year only
        Args:
           df (pandas dataframe): the dataframe for the original source data
           **kwargs (dict) : keyword arguments, needed as this method gets called generically via allowed_operations
        Returns:
           pandas dataframe: with one column in datatime format only displaying the year 
        """
        #raise an error if we dont have column in the kwargs
        #column == field
        if 'column' not in kwargs:
            raise ValueError(f'column not found in kwargs: {kwargs}')
        #get the pandas series of the input dataframe (get the field of the input dataset) to be modified
        series = df[kwargs['column']]
        # - convert the series into a datetime series
        # - get the datetime object via .dt
        # - get only the year via the .dt object
        # - convert the series back into a pandas dataframe
        return pd.to_datetime(series).dt.year


    def initialise(self):
        """
        Class initialisation
        - load all the neccessary files that get passed in via the cli arguments
        - load all the inputs into pandas dataframes so we can manipulate 
        """
        if self.output_data_folder == None:
            self.set_output_folder("./data/")

        if self.df_term_mapping is None:
            self.df_term_mapping = self.load_term_mapping(init=True)

        if self.df_structural_mapping is None:
            raise NoStructuralMapping('No structural mapping has been defined so cant run anything')
            
        if self.map_input_files is None:
            raise NoInputData('No Input data has been loaded, so cant run anything!')


        if 'y' in self.df_structural_mapping['term_mapping'].unique():
            if self.df_term_mapping is None:
                self.logger.error('Found term_mapping is needed!')
                raise NoTermMapping("Your structural mapping is indicating there is term"
                                    "But you didnt specify a source lookup for this via --term-mapping")
                    
        #perform a check that the source table names are consistent
        sm_source_tables = self.df_structural_mapping['source_table'].unique()
        data_source_tables = self.map_input_files.keys()

        diff_tables = list(set(sm_source_tables) - set(data_source_tables))
        if len(diff_tables) > 0 :
            self.logger.warning("Names of sources in the structural mapping and source data are not matching up")
            self.logger.warning("Im going to try going to lower case in the structural mapping")
            self.logger.warning("This is because WhiteRabbit is a pile of toss")
            
            self.df_structural_mapping['source_table'] = self.df_structural_mapping['source_table'].str.lower()
            sm_source_tables = self.df_structural_mapping['source_table'].unique()
            diff_tables = list(set(sm_source_tables) - set(data_source_tables))

            if len(diff_tables) > 0:
                #try with .csv on the end
                self.logger.warning('Still some different tables, see...')
                self.logger.warning(diff_tables)
                self.logger.warning(data_source_tables)
                self.logger.warning('Attempting looking with .csv in the name')
                
                self.df_structural_mapping['source_table'] = self.df_structural_mapping['source_table']\
                                                                 .apply(lambda x: f'{x}.csv')
                
                sm_source_tables = self.df_structural_mapping['source_table'].unique()
                diff_tables = list(set(sm_source_tables) - set(data_source_tables))

                if len(diff_tables) > 0 :
                    self.logger.warning('Still some different tables, see...')
                    self.logger.warning(diff_tables)
                    self.logger.warning(data_source_tables)
                    
                    self.logger.warning('Attempting now to match and rename')
                    
                    rename = {}
                    for bad_name in diff_tables:
                        for orig_name in data_source_tables:
                            if bad_name in orig_name:
                                rename[bad_name] = orig_name
                                break

                    self.df_structural_mapping = self.df_structural_mapping.replace({'source_table':rename})

                    sm_source_tables = self.df_structural_mapping['source_table'].unique()
                    diff_tables = list(set(sm_source_tables) - set(data_source_tables))
                    if len(diff_tables) > 0 :
                        self.logger.error("Still bad!!!")
                        self.logger.error('This means you are trying to map these datasets, but dont have the as input!')
                        raise BadStructuralMapping('Missing inputs OR .. your structural mapping must be misconfigured / not meant for this data')
                    else:
                        self.logger.warning("This could be bad! Only found by trying to match string names")
                else:
                    self.logger.warning("Found the names by appending .csv to the name")
            else:
                self.logger.warning("Ok found them by using str.lower() on the table names!")
        
        self.destination_tables = self.get_destination_tables()

        #perform a check that all the output tables are actually valid CDM objects
        cdm_objects = list(self.df_cdm.index.unique())

        for table in self.destination_tables:
            if table not in cdm_objects:
                self.logger.error(f'Found "{table}" in {self.f_structural_mapping}')
                self.logger.error(f'"{table}" not a cdm object')
                self.logger.error('Printing cdm objects...')
                self.logger.error(cdm_objects)
                raise KeyError(f'For some reason you are looking up "{table}", which has been specified in your structural mapping ({ self.f_structural_mapping}) but this isnt a valid CDM object ')

        
        self.logger.info(f'Destination tables to create... {list(self.destination_tables)}')
        self.logger.info(f'Done with tool initialisation...')
        self.tool_initialised = True
    
    def __init__(self):
        """
        Class __init__
        - setup the tool
        """
        #some initial parameters
        self.verbose = False
        self.chunk_size = 10**6
        self.max_chunks = -1 
        self.output_data_folder = None
        self.df_term_mapping = None
        self.df_structural_mapping = None
        self.map_output_data = None
        self.map_input_files = None
        self.tool_initialised = False


        #save a map for indices, this could be loaded from structural mapping
        #or we could have this as a separate input
        self.map_indexer = {}
        
        #configure how to save files
        self.save_files = True
        self.merge_files = True
        self.record_duplicates = False

        #default is to mask person_ids
        self.perform_person_id_mask = False
        #default is to automatically try and map fields e.g. year_of_birth --> extract year
        self.use_auto_functions = True
        # Fill in the blanks for testing some ids
        self.patch_missing_ids = True
        # Overide source term mapping
        # * if the destination field is called _source_
        #   and term mapping is defined... skip it... 
        self.override_source_term_mapping = False
        # more aggressively drop nan columns or not
        self.aggressive_drop = False
        
        #create a logger
        self.create_logger()
        
        #setup the working directory path so we can find the data
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.dir_path = os.path.abspath(os.path.join(self.dir_path,".."))
        self.logger.debug(f'Directory path... {self.dir_path}')

        #hard code OMPO CDM 5.3.1 for now
        f_cdm = f'{self.dir_path}/cdm/OMOP_CDM_v5_3_1.csv'
        if not os.path.isfile(f_cdm):
            raise FileNotFoundError(f'Cannot find the OMOP CDM v5.3.1 lookup file, looking here...  {f_cdm}')
        
        self.load_cdm(f_cdm)
        #define some types for the cdm map, to make sure our outputs are in the correct format
        self.cdm_dtypes = {
            'INTEGER': lambda x : x.astype('Int64'),
            'FLOAT' : lambda x : x.astype('Float64'),
            'VARCHAR': lambda x : x.fillna('').astype(str).apply(lambda x: x[:50]),
            'STRING(50)': lambda x : x.fillna('').astype(str).apply(lambda x: x[:50]),
            'DATETIME': lambda x : pd.to_datetime(x,errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S'),
            'DATE': lambda x : pd.to_datetime(x,errors='coerce').dt.date
        }
        
        self.allowed_operations = ETLOperations()

    
    def process_destination_table(self,destination_table):
        """
        Process a destination table (an output table in the cdm)
        """
        self.logger.info(f'Now running on Table "{destination_table}"')

        #create a list that will help track the output files we create
        output_files = []
        
        #load the CDM for this destination_table, e.g. patient
        partial_cdm = self.df_cdm.loc[destination_table]
        
        self.logger.info(f'Loaded the CDM for {destination_table}')
        self.logger.debug(json.dumps(partial_cdm['field'].to_list(),indent=4))
        
        #get a list of all 
        destination_fields = partial_cdm['field']
        mapped_fields = self.get_mapped_fields(destination_table)

        #save some information about index mapping, e.g. person_id
        if 'source_field_indexer' in self.df_structural_mapping:
            indices = self.df_structural_mapping['source_field_indexer'] == True
            indexer = self.df_structural_mapping[indices]\
                .set_index('destination_field')['source_field']\
                .to_dict()
            for key,val in indexer.items():
                indexer[key] = val.lower()
            
            self.logger.info(f'For table "{destination_table}" you have set the index for joining tables to be...')
            self.logger.info(indexer)
            self.map_indexer[destination_table] = indexer

        
        bad = []
        for x in mapped_fields:
            if x not in list(destination_fields):
                bad.append(x)
                self.logger.error(f'BAD INPUT -- SKIPPING {x}')
                self.logger.error(f'You are trying to map a field "{x}" that is not in this CDM!')
                self.logger.warning(f'Fields are called ... {list(destination_fields)}')
        mapped_fields = [x for x in mapped_fields if x not in bad]
                           
        unmapped_fields = list(set(destination_fields) - set(mapped_fields))

        self.logger.info(f'The CDM for "{destination_table}" has {len(destination_fields)}, you have mapped {len(mapped_fields)} leaving {len(unmapped_fields)} fields unmapped')
        all_destination_fields = json.dumps(list(destination_fields),indent=4)
        self.logger.debug(f'All fields \n {all_destination_fields}')
        all_mapped_fields = json.dumps(list(mapped_fields),indent=4)
        self.logger.debug(f'Mapped fields \n {all_mapped_fields}')
        all_unmapped_fields = json.dumps(list(unmapped_fields),indent=4)
        self.logger.debug(f'Unmapped fields \n {all_unmapped_fields}')

        source_tables = self.get_source_tables(destination_table)
        
        all_source_tables = json.dumps(source_tables,indent=4)
        self.logger.debug(f'All source tables needed to map {destination_table} \n {all_source_tables}')


        
        if len(source_tables) > 1:
            self.logger.debug(f'OK more than two tables mapping to the CDM "{destination_table}"')
            
        
        for source_table in source_tables:
            #structural mapping associated with the destination table and the source table
            df_mapping = self.get_structural_mapping(destination_table,source_table)
            
            #load the data we need
            #load in chunks to conserve memory when we have huge inputs
            chunks_table_data = self.load_df_chunks(self.map_input_files[source_table],
                                                    self.chunk_size)
        
            #start looping over the chunks of data
            #the default will be to have ~100k rows per chunk
            for icounter,df_table_data in enumerate(chunks_table_data):
                if self.max_chunks > 0 :
                    if icounter >= self.max_chunks :
                        self.logger.info('youve had enough')
                        break
                
                #use lower case to be safe because of WhiteRabbit Issues...
                df_table_data.columns = df_table_data.columns.str.lower()
                nrows = len(df_table_data)
                self.logger.info(f'Processing {icounter} with length {nrows}')

                primary_key = None
                if destination_table in self.map_indexer:
                    indices = list(self.map_indexer[destination_table].values())
                    if len(indices)> 1:
                        self.logger.error('too many indices set')
                        raise BadPrimaryKeyDefined('Youve set multiple primary keys.'
                                                   ' Not allowed yet!')
                    elif len(indices) ==1 :
                        primary_key = indices[0]
                        if primary_key in df_table_data.columns:
                            #clone the index to be this column
                            df_table_data.index = df_table_data[primary_key]
                            self.logger.info(f'Managed to set the index {primary_key} for {source_table}')
                        else:
                            self.logger.error(f'Attempting to set {primary_key}, which is not in {df_table_data.columns}')
                            self.logger.error(f'Currently working on {self.map_input_files[source_table]}')
                            raise BadPrimaryKeyDefined(f'Not able to find primary key in the table')
                else:
                    raise NoPrimaryKeyDefined(f"No primary key defined for {destination_table} "
                                              f"in {source_table}")
                            
                columns_output = {}
                
                mapped_fields_for_current_source_table = df_mapping.index.unique().to_list()

                #now start the real work of making new columns based on the mapping rules
                for destination_field in mapped_fields_for_current_source_table:
                    self.logger.info(f'Working on {destination_field}')

                    
                    #get all rules associated with the current field in the cdm 
                    rules = df_mapping.loc[[destination_field]]
                    #loop over all rules
                    for irule in range(len(rules)):
                        rule = rules.iloc[irule]
                        source_field = rule['source_field'].lower()

                        ret = None
                        
                        #perform a check to see if a source value is being mapped still
                        if "_source_value" in destination_field\
                           and not '_source_concept_id' in destination_field:

                            if rule['term_mapping'] == 'y':
                                self.logger.error('You have term mapping applied for'
                                                  f' the field {destination_field}'
                                                  ' are you sure!?'
                                                  ' This should be a source value!')
                                if self.override_source_term_mapping:
                                    rule['term_mapping'] = 'n'
                        
                        #handle when no term mapping
                        if rule['term_mapping'] == 'n':
                            self.logger.debug("No mapping term defined for this rule")
                            self.logger.debug(rule)
                            #map one-to-one if there isn't a rule
                            if rule['operation'] == 'n' or rule['operation'] == 'NONE' :
                                self.logger.debug("No operation set. Mapping one-to-one")

                                if self.use_auto_functions\
                                   and destination_field in self.allowed_operations.auto_functions:
                                    
                                    self.logger.debug("But found an auto function to use!")
                                    ret = self.map_auto_extract(df_table_data,
                                                                source_field,
                                                                destination_field)
                                else:
                                    ret = self.map_one_to_one(df_table_data,
                                                              source_field,
                                                              destination_field)
                            #there is an operation defined,
                            #so look it up in the list of allowed operations
                            #and apply it
                            else:
                                operation = rule['operation']
                            
                                if operation not in self.allowed_operations.keys():
                                    raise ValueError(f'Unknown Operation {operation}')
                                self.logger.debug(f'Applying {operation}')
                                ret = self.allowed_operations[operation](df_table_data,
                                                                         column=source_field,
                                                                         orig_column=source_field)

                                ret = ret.to_frame(destination_field)
                        #apply term mapping
                        else:
                            rule_id = rule['rule_id']
                            self.logger.debug(f'Mapping term found. Applying..')
                            self.logger.debug(f'{rule.to_dict()}')
                            df_map = self.df_term_mapping.loc[[rule_id]]
                            ret = self.map_via_rule(df_table_data,
                                                    df_map,
                                                    source_field,
                                                    destination_field)

                            operation = rule['operation']
                            if operation in  self.allowed_operations:
                                ret = self.allowed_operations[operation](ret,
                                                                         column=destination_field,
                                                                         orig_column=source_field)
                                ret = ret.to_frame(destination_field)

                                

                        ret = ret.sort_index()
                        ret['irule'] = irule
                        
                        self.logger.debug(ret)

                        
                    
                        if irule < 1:
                            columns_output[destination_field] = ret
                        else:
                            columns_output[destination_field] = pd.concat(
                                [columns_output[destination_field],ret])
                                                        
                            

                #concat all columns we created
                self.logger.info('Now setting up the inputs to merge')


                shapes = list(set([x.shape[0] for x in columns_output.values()]))
                min_shape = min(shapes)

                concat_list = [x for x in columns_output.values() if x.shape[0] == min_shape]
                                
                df_destination = pd.concat(concat_list,axis=1).drop('irule',axis=1)

                join_list = [
                    x.reset_index().set_index([x.index.name,'irule'])
                    for x in columns_output.values()
                    if x.shape[0] > min_shape
                ]

                if len(join_list)>0:
                    try:
                        df = pd.concat(join_list,axis=1)
                    except ValueError as err:
                        self.logger.error(err)
                        names = [x.columns[0] for x in join_list]
                        self.logger.error(f'Bad Merge for {names}')
                        self.logger.error('The most likely reason is that '
                                          'you have missed or duplicated a structural mapping'
                        )
                        self.logger.error('One or more of the following have a different number of rules set')
                        for x in join_list:
                            name = x.columns[0]
                            nunique = len(x.reset_index()['irule'].unique())
                            self.logger.error(f'col "{name}" has {nunique} unique rules')
                        raise BadJoin('Bad join of multiple mapping rules')
                    
                    
                    df.index = df.index.droplevel(1)
                    df = df.sort_index().dropna()

                    df_destination = df_destination.join(df)

                df_destination = df_destination.dropna(thresh=2).sort_index()
                self.logger.debug(df_destination)

                self.logger.info(f'chunk[{icounter}] completed: Final dataframe with {len(df_destination)} rows and {len(df_destination.columns)} columns created')

                
                #since we are looping over chunks
                #- for the first chunk, save the headers and set the write mode to write
                #- for the rest of the chunks, dont save the headers and set the write mode to append
                mode = 'w'
                header = True
                if icounter > 0:
                    mode = 'a'
                    header = False
                    
                self.logger.debug(f'writing mode:"{mode}", save headers="{header}')
            
                
                #save the data into new csvs
                outname = f'{self.output_data_folder}/cdm_split/{destination_table}/'

                if not os.path.exists(outname):
                    self.logger.info(f'Creating a new folder: {outname}')
                    os.makedirs(outname)

                #clean up the name to save as a csv file
                outname = f'{outname}/{source_table}'
                if outname[-4:]!='.csv':
                    outname += '.csv'
                df_destination.to_csv(outname,index=False,\
                                      mode=mode,header=header)

                self.logger.info(f'Saved final csv with data mapped to CDM5.3.1 here: {outname}')

                #only need to do this one, since for icounter>0 the file is in append mode
                #rather than in write mode
                if icounter == 0 :
                    output_files.append(outname)

        return output_files


    def merge_destination_table(self,destination_table,outputs):
        self.logger.info(f'Merging {destination_table}')
        
        #retrieve the fields that should be associated with this CDM
        cdm_fields = self.df_cdm.loc[destination_table]['field'].tolist()
        
        #load all the output files, in chunk format, to not overload memory
        #by loading all up at the same time
        chunks_output_file_map = {
            output_file: self.load_df_chunks(output_file)
            for output_file in outputs
        }
        
        #slightly complex way looping over the output files associated with a cdm
        #but doing this in chunks... i.e 10k lines at a time
        complete = False
        icounter = 0 
        while not complete:
            total = []
            for output_file,chunks in chunks_output_file_map.items():
                try:
                    df_chunk = chunks.get_chunk()
                    df_chunk.columns = df_chunk.columns.str.replace("(\.\d+)$", "")
                    total.append(df_chunk)
                except StopIteration:
                    complete = True
                    break
                
                self.logger.debug(f'.. loading the file {output_file}')
            
            if complete:
                break

            
            #make a total dataframe
            df_output = pd.concat(total,axis=1)

            
            #get all unique columns
            unique_cols = df_output.columns.unique()
            missing_cols = list(set(cdm_fields) - set(unique_cols))
        
            #create nan columns for unmapped (missing) fields for this cdm
            for missing_field in missing_cols:
                df_output[missing_field] = np.nan
            
            self.logger.debug(f"Missing columns: {missing_cols}")
            
            
            #define how to save the output file again
            #- on the first loop (of chunks): write the headers and use write mode (recreate the file)
            #- on other loops: dont write the headers but write in append mode 
            mode = 'w'
            header = True
            if icounter > 0:
                mode = 'a'
                header = False
                
            #check for duplicate columns
            duplicate_cols = df_output.columns[df_output.columns.duplicated()].unique()

            if len(duplicate_cols)>0:
                self.logger.warning("You've got duplicated columns for this cdm")
                self.logger.warning(f'Duplicated: {duplicate_cols}')
                
            output_duplicates = []
            for duplicate in duplicate_cols:
                
                first = df_output[duplicate].iloc[:,0]
                others =  df_output[duplicate].iloc[:,1:]
                
                #drop columns that have been duplicated
                df_output = df_output.drop(duplicate,axis=1)
                #add back in only first one
                df_output[duplicate] = first
                
                #save the others in a seperate frame
                output_duplicates.append(others)
                
            outfolder = f'{self.output_data_folder}/cdm_merged'
            if not os.path.exists(outfolder):
                self.logger.info(f'Creating a new folder: {outfolder}')
                os.makedirs(outfolder)

            #rearrange the order of the columns so they're the same as the order in the CDM
            df_output = df_output[cdm_fields]

            
            #perform masking of the person id
            #- perfom is person_id is in the cdm and is not empty/null
            #- save the lookup to the person id
            #- make an arbritary index for the person id instead
            if self.perform_person_id_mask \
               and 'person_id' in df_output \
               and not df_output['person_id'].isnull().all():

                #raise NotImplementedError('need to fix masking of person_id still!')
                masker = {
                    x:i
                    for i,x in enumerate(sorted(df_output['person_id'].unique()))
                }
                self.save_lookup_table(masker,destination_table,'person_id')

                df_output['person_id'] = df_output['person_id'].replace(masker)

           
            cdm = self.df_cdm.loc[destination_table][['field','required','type']]
            for i in range(len(cdm)):
                field = cdm.iloc[i]['field']
                required = cdm.iloc[i]['required']
                dtype = cdm.iloc[i]['type']

                if 'VARCHAR' in dtype:
                    dtype = 'VARCHAR'

                if required:
                    #check if an entire row has 
                    null_values = df_output[field].isnull().values
                    if null_values.all():
                        self.logger.error(f'Required field {field} has not been mapped'
                                          ' all values are NaN')

                        if self.patch_missing_ids:
                            if i == 0:
                                #if it's a primary key, increment index
                                df_output[field] = df_output[field].reset_index().index
                            else:
                                #if else, fill 0 
                                df_output[field] = 0
                        else:
                            raise MissingRequiredMapping(f'You need to map {field}')
                        
                    elif null_values.any() and self.aggressive_drop:
                        n_bad_indices = len(df_output[null_values].index)
                        n_indices = len(df_output.index)
                        df_output = df_output.loc[~null_values]
                        self.logger.warning(f'Required field ({field}) has bad values!')
                        self.logger.warning(f'Just had to drop {n_bad_indices}/{n_indices}')

                try:
                    df_output[field] = self.cdm_dtypes[dtype](df_output[field])
                except TypeError:
                    
                    self.logger.error(f'Cant convert column {field} to datatype {dtype}')
                    self.logger.error(f'\n f{df_output[[field]]}')
                    
                    if required:
                        self.logger.warning(f'{field} is not required though, so dropping it')
                        df_output[field] = np.NaN
                        df_output[field] = self.cdm_dtypes[dtype](df_output[field])
                    else:
                        self.logger.error(f'This is required! And not a dtype {dtype}.'
                                          ' Please fix!')
                        
                        raise BadDestinationField(f'{field} is required, and needs to be {dtype}')


            outname = f'{outfolder}/{destination_table}.csv'
            
            df_output.to_csv(outname,index=False,mode=mode,header=header)
            if mode == 'w':
                self.logger.info(f'...saved to {outname}')
            else:
                self.logger.info(f'...appended to {outname}')


            if len(df_output) < 10:
                self.logger.debug(df_output)
            else:
                self.logger.debug(df_output.sample(10))
        
            #record where the output is
            if self.map_output_data is None:
                self.map_output_data = {}
            self.map_output_data[destination_table] = outname
        
            
            #record duplicates
            #- need to work out how to handle this better
            if len(output_duplicates)>0 and self.record_duplicates:
                df_duplicates = pd.concat(output_duplicates,axis=1)
                outname = f'{outfolder}/{destination_table}.duplicates.csv'
                df_duplicates.to_csv(outname,index=False,mode=mode,header=header)
                                        
            self.logger.debug('Merge of all source tables associated with cdm object complete')
            icounter +=1

            
    
    def run(self):
        """
        Start the program running by looping over the CDM destination tables defined by the user
        """
        self.logger.info('starting to run')

        #check if the tool was initialised or not
        if not self.tool_initialised:
            self.initialise()
        
        self.logger.info('Starting ETL to CDM')

        map_output_files = {}
        #loop over all CDM tables (e.g. person etc.)
        for destination_table in self.destination_tables:
            #process the table
            output_files = self.process_destination_table(destination_table)
            map_output_files[destination_table] =  output_files

        #merge output tables
        #for each CDM destination table
        #- get all new csv files we created
        #- we'll have one per source table
        #- merge them together
        for destination_table,outputs in map_output_files.items():
            self.merge_destination_table(destination_table,outputs)



        
