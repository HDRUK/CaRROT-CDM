import os
import pandas as pd
import numpy as np
import argparse
import logging
import coloredlogs
coloredlogs.DEFAULT_FIELD_STYLES['levelname']['color'] = 'white'

parser = argparse.ArgumentParser(description='Tool for mapping datasets')
parser.add_argument('--inputs','-i', nargs='+', required=True,
                    help='input .csv files for the original data to be mapped to the CDM')
parser.add_argument('--output-folder','-o', default='./data',
                    help='location of where to store the data')
parser.add_argument('--term-mapping','-tm', required=True,
                    help='file that will handle the term mapping')
parser.add_argument('--structural-mapping','-sm', required=True,
                    help='file that will handle the structural mapping')

parser.add_argument('-v','--verbose',help='set debugging level',action='store_true')


class ETLTool:
    """

    """
    def get_source_table_name(self, fname):
        """
        """
        fname = fname.split("/")[-1]
        self.logger.debug(f'Extracting the name of the table for input: {fname}')
        if fname[-4:] == '.csv': 
            return fname.split('.csv')[0]
        
        raise NotImplementedError(f"{fname} is not a .csv file. Don't know how to handle non csv files yet!")

    def get_df(self,fname,lower_case=True):
        """
        """
        df = pd.read_csv(fname)
        if lower_case:
            df.columns = df.columns.str.lower()
        return df

    def create_logger(self):
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

    def load_cdm(self):
        self.df_cdm = pd.read_csv(self.f_cdm,encoding="ISO-8859-1").set_index('table')
        self.logger.debug(self.df_cdm)
        
    def load_input_data(self):
        self.map_input_data = { 
            self.get_source_table_name(fname): self.get_df(fname)
            for fname in self.f_inputs
        }
        self.logger.info(f'found the following input tables: {list(self.map_input_data.keys())}')

    def load_structural_mapping(self):
        self.df_structural_mapping = self.get_df(self.f_structural_mapping)\
                                         .set_index(['destination_table','rule_id'])
        self.logger.debug(self.df_structural_mapping)
        self.logger.info(f'Loaded the structural mapping with {len(self.df_structural_mapping)} rules')
        
                
    def load_term_mapping(self):
        self.df_term_mapping = self.get_df(self.f_term_mapping)\
                                         .set_index(['rule_id'])
        self.logger.debug(self.df_term_mapping)
        self.logger.info(f'Loaded the term mapping with {len(self.df_term_mapping)} rules')


    def save_lookup_table(self,df,table,source_field):
        series = df[source_field].rename(f'source_{source_field}')
        series.to_csv(f'{self.output_data_folder}/{table}-{source_field}.csv',
                      index_label=f'destination_{source_field}')

        
    def get_output_tables(self):
        return self.df_structural_mapping.index.get_level_values(0).unique()

    def get_mapped_fields(self,table):
        return list(self.df_structural_mapping.loc[table]['destination_field'])

    def get_structural_mapping(self,table):
        return self.df_structural_mapping.loc[table].reset_index().set_index('destination_field')
    
    def get_source_table(self,table):
        retval = self.df_structural_mapping.loc[table]['source_table'].unique()
        if len(retval)>1:
            raise ValueError('Something really wrong if there are different source tables for this mapping')
        return retval[0]

    def map_one_to_one(self,df,source_field,destination_field):
        return df[source_field].to_frame(destination_field)

    def get_year_from_date(self,df,**kwargs):
        series = df[kwargs['column']]
        return pd.to_datetime(series).dt.year
    
    def __init__(self,args):
        """
        
        """
        self.verbose = args.verbose
        self.create_logger()
        self.f_inputs = args.inputs
        self.f_term_mapping = args.term_mapping
        self.f_structural_mapping = args.structural_mapping
        self.output_data_folder = args.output_folder
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.logger.debug(f'Directory path... {self.dir_path}')
        self.f_cdm = f'{self.dir_path}/cdm/OMOP_CDM_v5_3_1.csv'


        self.allowed_operations = {'extract year': self.get_year_from_date}
        
        self.load_cdm()
        self.load_input_data()
        self.load_structural_mapping()
        self.load_term_mapping()
        
        self.output_tables = self.get_output_tables()
        self.logger.info(f'Output tables to create... {list(self.output_tables)}')

    
    def process_table(self,table):
        """
        Process a table
        """
        self.logger.info(f'Now running on Table "{table}"')

        partial_cdm = self.df_cdm.loc[table]
        self.logger.info('Loaded the CDM for this table which has the following fields..')
        output_fields = partial_cdm['field']
        
        mapped_fields = self.get_mapped_fields(table)

        for x in mapped_fields:
            if x not in list(output_fields):
                raise ValueError(f'You are trying to map a field "{x}" that is not in this CDM! Fields are called ... {list(output_fields)}')
        
        unmapped_fields = list(set(output_fields) - set(mapped_fields))

        self.logger.info(f'The CDM for "{table}" has {len(output_fields)}, you have mapped {len(mapped_fields)} leaving {len(unmapped_fields)} fields unmapped')
        self.logger.debug(f'All fields {list(output_fields)}') 
        self.logger.debug(f'Mapped fields {list(mapped_fields)}')


        source_table = self.get_source_table(table)
        df_mapping = self.get_structural_mapping(table)
        df_table_data = self.map_input_data[source_table]

        df_table_data_blank = df_table_data.reset_index()[['index']]
        
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
                if rule['operation'] == 'n':
                    self.logger.debug("Mapping one-to-one")
                    columns_output.append(
                        self.map_one_to_one(df_table_data,source_field,destination_field)
                    )
                else:
                    operation = rule['operation']
                    if operation not in self.allowed_operations.keys():
                        raise ValueError(f'Unknown Operation {operation}')
                    self.logger.debug(f'applying {operation}')
                    series = self.allowed_operations[operation](df_table_data,column=source_field)
                    columns_output.append(
                        series.to_frame(destination_field)
                    )
            else:
                rule_id = rule['rule_id']
                self.logger.debug(f'applying {rule}')
                columns_output.append(
                    df_table_data[[source_field]]\
                    .merge(self.df_term_mapping.loc[rule_id],
                           left_on=source_field,
                           right_on='source_term',
                           how='left')\
                    [['destination_term']].rename({'destination_term':destination_field},axis=1)
                )

        
        df_destination = pd.concat(columns_output,axis=1)
        df_destination = df_destination[output_fields]

        df_destination_final = df_destination.drop('person_id',axis=1)\
                        .reset_index()\
                        .rename({'index':'person_id'},axis=1)

        df_destination_final.to_csv('output.csv',index=False)


    def run(self):
        """
        """
        self.logger.info('Starting ETL to CDM')
        for output_table in self.output_tables:
           self.process_table(output_table) 

        
if __name__ == '__main__':
    args = parser.parse_args()
    runner = ETLTool(args)
    runner.run()
