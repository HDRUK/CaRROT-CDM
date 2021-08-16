import pandas as pd
from coconnect.tools.logger import Logger

class DataBrick:
    def __init__(self,df_handler):
        self.__df_handler = df_handler
        self.__df = None

    def get_chunk(self,chunksize):
        #if the df handler is a TextFileReader, get a dataframe chunk
        if isinstance(self.__df_handler,pd.io.parsers.TextFileReader):
            try:
                #for this file reader, get the next chunk of data 
                self.__df = self.__df_handler.get_chunk(chunksize)
            except StopIteration:
                #otherwise, if at the end of the file reader, return an empty frame
                self.__df = pd.DataFrame(columns=self.__df.columns)
        elif self.__df is not None:
            #if we're handling non-chunked data
            #return an empty dataframe if we've already loaded this dataframe
            self.__df = pd.DataFrame(columns=self.__df.columns)
        else:
            #otherwise return the dataframe as it's the first time we're getting it
            self.__df = self.__df_handler

    def get_df(self):
        return self.__df
        
class DataCollection:
    def __init__(self,file_map=None,chunksize=None):
        self.chunksize = chunksize
        
        self.__bricks = {}

        self.logger = Logger(self.__class__.__name__)
        self.logger.info("InputData Object Created")
        if self.chunksize is not None:
            self.logger.info(f"Using a chunksize of '{self.chunksize}' nrows")

        if file_map is not None:
            self._load_input_files(file_map)
            
    def _load_input_files(self,file_map):
        for name,path in file_map.items():
            df = pd.read_csv(path,
                             chunksize=self.chunksize,
                             dtype=str)
            self[name] = DataBrick(df)

    def all(self):
        return {
            key:self[key]
            for key in self.keys()
        }
        
    def keys(self):
        return self.__bricks.keys()

    def items(self):
        return self.__bricks.items()

    def next(self):
        #loop over all loaded files
        for key,brick in self.items():
            self.logger.debug(f"Getting the next chunk of size '{self.chunksize}' for '{key}'")
            brick.get_chunk(self.chunksize)

        #check if all __dataframe objects are empty
        #if they are, raise a StopIteration as processing has finished
        if all([x.get_df().empty for x in self.__bricks.values()]):
            self.logger.debug("All input files have now been processed.")
            raise StopIteration
        
        if self.chunksize is not None:
            self.logger.info(f"Moving onto the next chunk of data (of size {self.chunksize})")

        
    def __getitem__(self,key):
        brick = self.__bricks[key]
        df = brick.get_df()
        if df is None:
            brick.get_chunk(self.chunksize)
            df = brick.get_df()
        return df
                
    def __setitem__(self,key,obj):
        if not isinstance(obj,DataBrick):
            raise NotImplementedError("When using InputData, the object must be of type "
                                      f"{DataBrick}")
        self.logger.info(f"Registering  {key} [{type(obj)}]")
        self.__bricks[key] = obj
        
    
    

