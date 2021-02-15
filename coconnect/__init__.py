"""ETLTool

A program for mapping ETL to CDM based on input datasets, structual mapping and term mapping csv files

Contact: CO-CONNECT@dundee.ac.uk
First Created: 11/02/2021

Example:
    Examples can be given using either the ``Example`` or ``Examples``
    sections. Sections support any reStructuredText formatting, including
    literal blocks::

        $ python example_google.py

Test

Attributes:
    module_level_variable1 (int): Module level variables may be documented in
        either the ``Attributes`` section of the module docstring, or in an
        inline docstring immediately following the variable.

        Either form is acceptable, but the two should not be mixed. Choose
        one convention to document module level variables and be consistent
        with it.

Todo:
    * For module TODOs
    * You have to also use ``sphinx.ext.todo`` extension

.. _Google Python Style Guide:
   http://google.github.io/styleguide/pyguide.html


"""

import logging
import coloredlogs
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'
import os
import pandas as pd
import numpy as np

class NoInputData(Exception):
    pass
class NoStructuralMapping(Exception):
    pass


class ETLTool:
    """
    A class for the ETLTool runner, this will handle the loading of the input files
    """
    def get_source_table_name(self, fname):
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
            return fname.split('.csv')[0]
        
        raise NotImplementedError(f"{fname} is not a .csv file. Don't know how to handle non csv files yet!")

    def load_df_chunks(self,fname,chunk_size=10**6):
        """
        Extract a pandas Dataframe from an input csv file
        Args:
           fname (str): the file name
           chunksize(int): specify how many rows to read in per chunk
        Returns: 
           Pandas TextFileReader
        """
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

    def create_logger(self):
        """
        Initialisation of a logging system for cli messages
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        if self.verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
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

        
    def set_verbose(self,verbose=True):
        """
        Set the message level of the tool to be verbose or not
        Args:
           verbose (bool):  whether to log messages to help debug or not
        """
        self.verbose = verbose

    def set_chunk_size(self,chunk_size):
        """
        Set the chunk size of the number of rows of a table to load into memory in each batch
        Args:
            chunk_size (long int) : the number of rows to process 
        """
        self.chunk_size = chunk_size
        
    def load_cdm(self,f_cdm):
        """
        Load the default cdm model (v5.3.1) into a pandas dataframe
        Args:
           None
        Returns:
           None
        """
        self.df_cdm = pd.read_csv(f_cdm,encoding="ISO-8859-1").set_index('table')
        self.logger.debug(self.df_cdm)
        
    def load_input_data(self,f_inputs):
        """
        Load the input data, which is a list of input files into pandas dataframes
        and map, based on the original file name, to the dataframe: {name: dataframe}
        Args:
           None
        Returns:
           None
        """
        self.map_input_data = { 
            self.get_source_table_name(fname): self.load_df_chunks(fname,self.chunk_size)
            for fname in f_inputs
        }
        self.logger.info(f'found the following input tables: {list(self.map_input_data.keys())}')

    def load_structural_mapping(self,f_structural_mapping):
        """
        Load the structural mapping into a pandas dataframe

        Args:
           None
        Returns:
           None
        """
        #set the index to be a multindex of ['destination_table','rule_id']
        #this is just going to help us out down the line
        #when we use df_structural_mapping.loc[table_name] to easily extract
        #structural mapping rules for associated with the given source table
        
        self.df_structural_mapping = self.load_df(f_structural_mapping)\
                                         .set_index(['destination_table','rule_id'])
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
        
        self.df_term_mapping = self.load_df(f_term_mapping)\
                                         .set_index(['rule_id'])
        self.logger.debug(self.df_term_mapping)
        self.logger.info(f'Loaded the term mapping with {len(self.df_term_mapping)} rules')


    def save_lookup_table(self,df,source_table,table,source_field,mode='w',header=True):
        """
        Save a dataframe series in a table to a file
        Args:
           None
        Returns:
           None
        """

        series = df[source_field].rename(f'source_{source_field}')
        oname = f'{self.output_data_folder}/lookup_{source_table}_{table}_{source_field}.csv'
        self.logger.info(f'Writing a lookup dictionary of {source_field} to index')
        self.logger.info(f'Final being saved: {oname}')
        series.to_csv(oname,
                      index_label=f'destination_{source_field}',
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
        return self.df_structural_mapping.index.get_level_values(0).unique()

    def get_mapped_fields(self,table):
        """
        Get the names all the destination fields that are associated with the structural mapping of a partiular table
        Args:
           table (str): name of a table
        Returns:
           list
        """
        return list(self.df_structural_mapping.loc[table]['destination_field'])

    def get_structural_mapping(self,table):
        """
        Gets the dataframe for the structural mapping and filtered by the table name
        then sets the index to be the destination field (for later convienience 
        Args:
           table (str): name of a table
        Returns:
           pandas.DataFrame : a new pandas dataframe with 'destination_field' as the index
        """
        return self.df_structural_mapping.loc[table].reset_index().set_index('destination_field')
    
    def get_source_table(self,table):
        """
        Gets the dataframe for the structural mapping and filtered by the table name
        then looks up all the unique source tables associated with this structural mapping
        there really shouldn't be more than one source table associated to the (destination) table
        Args:
           table (str): name of a table
        Returns:
           str: the name of the source table associated with this mapping
        """
        retval = self.df_structural_mapping.loc[table]['source_table'].unique()
        if len(retval)>1:
            raise ValueError('Something really wrong if there are different source tables for this mapping')
        return retval[0]


    def map_via_rule(self,df,df_map,source_field,destination_field,drop_bad=True):
        df_orig = df[[source_field]]
        df_orig = df_orig.merge(df_map,
                                left_on=source_field,
                                right_on='source_term',
                                how='left')
        df_orig = df_orig[['destination_term']].rename(
            {
                'destination_term':destination_field
            },axis=1)

        df_bad = df_orig.index[df_orig.isnull().any(axis=1)]
        if len(df_bad) > 0 :
            self.logger.warning(f'Found bad row {df_bad}')
            self.logger.warning(df[[source_field]][df_orig.isnull().any(axis=1)].to_dict())
            self.logger.warning('This has no specification of how to map it')
            self.logger.warning(f'Options are: \n {df_map}')
            #add a switch to drop any rows that have a nan value aka the term mapping failed
            if drop_bad:
                df_orig = df_orig.dropna()
                
        return df_orig
        
    
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

    def get_month_from_date(self,df,**kwargs):
        """
        Convert a dataframe containing a datatime into the month only
        Args:
           df (pandas dataframe): the dataframe for the original source data
           **kwargs (dict) : keyword arguments, needed as this method gets called generically via allowed_operations
        Returns:
           pandas dataframe: with one column in datatime format only displaying the month 
        """
        #raise an error if we dont have column in the kwargs
        #column == field
        if 'column' not in kwargs:
            raise ValueError(f'column not found in kwargs: {kwargs}')
        #get the pandas series of the input dataframe (get the field of the input dataset) to be modified
        series = df[kwargs['column']]
        # - convert the series into a datetime series
        # - get the datetime object via .dt
        # - get only the month via the .dt object
        # - convert the series back into a pandas dataframe
        return pd.to_datetime(series).dt.month.to_frame()

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
            
        if self.map_input_data is None:
            raise NoInputData('No Input data has been loaded, so cant run anything!')
            
        
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
        self.output_data_folder = None
        self.df_term_mapping = None
        self.df_structural_mapping = None
        self.map_input_data = None
        self.tool_initialised = False
        
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

        

        #hard code for now..
        #map lookup name with a function to perform an operation on a field(s)
        self.allowed_operations = {
            'extract year': self.get_year_from_date,
            #'extract month': self.get_month_from_date
        }
        

    
    def process_table(self,table):
        """
        Process a destination table
        """
        self.logger.info(f'Now running on Table "{table}"')

        #load the CDM for this table, e.g. patient
        #note - need to add a catch here to make sure the table is valid in the CDM
        partial_cdm = self.df_cdm.loc[table]
        
        self.logger.info('Loaded the CDM for this table which has the following fields..')

        #get a list of all 
        destination_fields = partial_cdm['field']
        mapped_fields = self.get_mapped_fields(table)

        for x in mapped_fields:
            if x not in list(destination_fields):
                raise ValueError(f'You are trying to map a field "{x}" that is not in this CDM! Fields are called ... {list(destination_fields)}')
        
        unmapped_fields = list(set(destination_fields) - set(mapped_fields))

        self.logger.info(f'The CDM for "{table}" has {len(destination_fields)}, you have mapped {len(mapped_fields)} leaving {len(unmapped_fields)} fields unmapped')
        self.logger.debug(f'All fields {list(destination_fields)}') 
        self.logger.debug(f'Mapped fields {list(mapped_fields)}')


        source_table = self.get_source_table(table)
        df_mapping = self.get_structural_mapping(table)

        if source_table not in self.map_input_data:
            raise LookupError(f"You have specified a mapping for  \"{source_table}\" but this cannot be found in any of the input datasets: {self.map_input_data.keys()}")
        chunks_table_data = self.map_input_data[source_table]

        
        for icounter,df_table_data in enumerate(chunks_table_data):
            df_table_data.columns = df_table_data.columns.str.lower()
            
            self.logger.debug(f'Processing {icounter}')
            
            df_table_data_blank = pd.DataFrame({'index':range(len(df_table_data))})
            columns_output = []
            
            #first create nan columns for unmapped fields in the CDM
            for destination_field in unmapped_fields:
                df_table_data_blank[destination_field] = np.nan
                columns_output.append(df_table_data_blank[destination_field])

            #now start the real work of making new columns based on the mapping rules
            for destination_field in mapped_fields:
                self.logger.info(f'Working on {destination_field}')
                rule = df_mapping.loc[destination_field]
                source_field = rule['source_field']
                if rule['term_mapping'] == 'n':
                    self.logger.debug("No mapping term defined for this rule")
                    if rule['operation'] == 'n':
                        self.logger.debug("No operation set. Mapping one-to-one")
                        columns_output.append(
                            self.map_one_to_one(df_table_data,source_field,destination_field)
                        )
                    else:
                        operation = rule['operation']
                        if operation not in self.allowed_operations.keys():
                            raise ValueError(f'Unknown Operation {operation}')
                        self.logger.debug(f'Applying {operation}')
                        ret = self.allowed_operations[operation](df_table_data,column=source_field)
                        columns_output.append(
                            ret.to_frame(destination_field)
                        )
                else:
                    rule_id = rule['rule_id']
                    self.logger.debug(f'Mapping term found. Applying..')
                    self.logger.debug(f'{rule.to_dict()}')
                    df_map = self.df_term_mapping.loc[rule_id]
                    columns_output.append(
                        self.map_via_rule(df_table_data,df_map,source_field,destination_field)
                    )
                    
        
            df_destination = pd.concat(columns_output,axis=1)
            self.logger.debug(f'concatenated the output columns into a new dataframe')
            df_destination = df_destination[destination_fields]
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
            
            #perform masking of the person id
            #- perfom is person_id is in the cdm and is not empty/null
            #- save the lookup to the person id
            #- make an arbritary index for the person id instead
            if 'person_id' in df_destination and not df_destination['person_id'].isnull().all():
                self.save_lookup_table(df_destination,source_table,table,'person_id')
                df_destination = df_destination.drop('person_id',axis=1)\
                                               .reset_index()\
                                               .rename({'index':'person_id'},axis=1)
                
            #save the data into new csvs
            outname = f'{self.output_data_folder}/mapped_{source_table}_{table}.csv'
            df_destination.to_csv(outname,index=False,mode=mode,header=header)
            self.logger.info(f'Saved final csv with data mapped to CDM5.3.1 here: {outname}')

            self.df_output = df_destination
            

    def run(self):
        """
        Start the program running by looping over the CDM destination tables defined by the user
        """
        if not self.tool_initialised:
            self.initialise()
        
        self.logger.info('Starting ETL to CDM')
        for destination_table in self.destination_tables:
           self.process_table(destination_table) 

