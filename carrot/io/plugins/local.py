import pandas as pd
from carrot.io.common import DataCollection,DataBrick
import glob
import io
import os
import json
import pandas as pd
from time import gmtime, strftime
        
class LocalDataCollection(DataCollection):
    def __init__(self,file_map=None,chunksize=None,nrows=None,output_folder=None,sep=',',write_mode='w',write_separate=False,**kwargs):
        super().__init__(chunksize=chunksize,nrows=nrows)

        self.__output_folder = output_folder
        self.__separator = sep
        self.__write_mode = write_mode
        self.__write_separate = write_separate

        if file_map is not None:
            self._load_input_files(file_map)

    def get_output_folder(self):
        return self.__output_folder 
            
    def get_global_ids(self):
        if not self.__output_folder:
            return

        files = glob.glob(self.__output_folder+os.path.sep+"person_ids.*"+self.get_outfile_extension())
        return files
        
    def load_global_ids(self):
        if self.__write_mode == 'w':
            return

        files = self.get_global_ids()
        if not files:
            return
        
        self.logger.warning(f"Loading existing person ids from...")
        self.logger.warning(f"{files}")
        return pd.concat([pd.read_csv(fname,sep=self.__separator).set_index('TARGET_SUBJECT')['SOURCE_SUBJECT']
                          for fname in files
        ]).to_dict()

    def get_separator(self):
        return self.__separator
    
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

        
    def load_meta(self,name='.meta'):
        f_out = self.__output_folder
        fname = f"{f_out}{os.path.sep}{name}.json"
        if not os.path.exists(fname):
            return
        with open(fname,'r') as f:
            data = json.load(f)
            return data

    def load_indexing(self):
        meta = self.load_meta()
        if not meta:
            return
        
        indexing = {}
        for _,v in meta.items():
            v = v['meta']['total_data_processed']
            for k,n in v.items():
                if k not in indexing:
                    indexing[k] = 0
                indexing[k] += n
        return indexing
        
    def write_meta(self,data,name='.meta'):
        if not isinstance(data,dict):
            raise NotImplementedError(f"{type(data)} must be of type dict")

        data = {hex(id(data)):data}
        
        mode = self.__write_mode
        f_out = self.__output_folder
        if not os.path.exists(f'{f_out}'):
            self.logger.info(f'making output folder {f_out}')
            os.makedirs(f'{f_out}')

        fname = f"{f_out}{os.path.sep}{name}.json"
        if os.path.exists(fname) and mode == 'a':
            with open(fname,'r') as f:
                existing_data = json.load(f)
                data = {**existing_data,**data}
        #rewrite it
        with open(f"{f_out}{os.path.sep}{name}.json","w") as f:
            json.dump(data,f,indent=6)
            return
                    
    def write(self,name,df,mode='w'):

        f_out = self.__output_folder
        if not os.path.exists(f'{f_out}'):
            self.logger.info(f'making output folder {f_out}')
            os.makedirs(f'{f_out}')
        
        if mode == None:
            mode = self.__write_mode
            
        if self.__write_separate:
            time = strftime("%Y-%m-%dT%H%M%S", gmtime())
            if 'name' in df.attrs:
                name = name + '.' + df.attrs['name']
            name = name + "."+ hex(id(df)) + "." + time
            mode = 'w'


            
        file_extension = self.get_outfile_extension()
        fname = f'{f_out}{os.path.sep}{name}.{file_extension}'
        #force mode to write if the file doesnt exist yet
        if not os.path.exists(fname):
            mode = 'w'
            
        header=True
        if mode == 'a':
            header = False
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
                             nrows=self.nrows,
                             dtype=str)
            self[name] = DataBrick(df)

    def load_input_dataframe(self,file_map):
        for name,df in file_map.items():
            self[name] = DataBrick(df)
    
                
        
    
    

