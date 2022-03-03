import pandas as pd
from coconnect.io.common import DataCollection,DataBrick
import io
import os

        
class LocalDataCollection(DataCollection):
    def __init__(self,file_map=None,chunksize=None,output_folder=None,sep=',',write_mode='w',**kwargs):
        super().__init__(chunksize=chunksize)

        self.__output_folder = output_folder
        self.__separator = sep
        self.__write_mode = write_mode

        if file_map is not None:
            self._load_input_files(file_map)

    def get_output_folder(self):
        return self.__output_folder 
            
    def get_global_ids(self):
        if not self.__output_folder:
            return
        
        global_id_fname = self.__output_folder+os.path.sep+"global_ids."+self.get_outfile_extension()
        if os.path.exists(global_id_fname):
            return global_id_fname
        else:
            return

    def load_global_ids(self):
        if self.__write_mode == 'w':
            return

        if global_id_fname := self.get_global_ids():
            _df = pd.read_csv(global_id_fname,sep=self.__separator).set_index('TARGET_SUBJECT')['SOURCE_SUBJECT']
            return _df.to_dict()
            
    def get_outfile_extension(self):
        """
        Work out what the extension of the output file for the dataframes should be.

        Given the '_outfile_separator' to be used in `df.to_csv`,
        work out the file extension.

        At current, only tab separated and comma separated values (files) are supported
        
        Returns:
           str: outfile extension name
        
        """
        if self.__separator == ',':
            return 'csv'
        elif self.__separator == '\t':
            return 'tsv'
        else:
            self.logger.warning(f"Don't know what to do with the extension '{self.__separator}' ")
            self.logger.warning("Defaulting to csv")
            return 'csv'

            
    def write(self,name,df,mode='w'):

        if mode == None:
            mode = self.__write_mode

        header=True
        if mode == 'a':
            header = False

        f_out = self.__output_folder
        file_extension = self.get_outfile_extension()
        
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
        df.to_csv(fname,mode=mode,header=header,index=True,sep=self.__separator)

        self.logger.debug(df.dropna(axis=1,how='all'))
        self.logger.info("finished save to file")
        return fname
            
    def _load_input_files(self,file_map):
        for name,path in file_map.items():
            df = pd.read_csv(path,
                             chunksize=self.chunksize,
                             dtype=str)
            self[name] = DataBrick(df)
    
                
        
    
    

