import pandas as pd
from coconnect.io.common import DataCollection

class DataBrick:
    def __init__(self,df_handler,name=None):
        self.name = name
        self.__df_handler = df_handler
        self.__df = None

    def get_handler(self):
        return self.__df_handler

    def reset(self):
        if isinstance(self.__df_handler,pd.io.parsers.TextFileReader):
            options = self.__df_handler.options
            f = self.__df_handler.f
            self.__df_handler = pd.io.parsers.TextFileReader(f,**options)
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
        else:
            #if we're handling non-chunked data
            if self.__df is not None:
                #return an empty dataframe if we've already loaded this dataframe
                self.__df = pd.DataFrame(columns=self.__df.columns)
            else:
                #otherwise return the dataframe as it's the first time we're getting it
                self.__df = self.__df_handler

    def get_df(self):
        return self.__df
        
class LocalDataCollection(DataCollection):
    def __init__(self,file_map=None,chunksize=None):
        super().__init__()
        self.chunksize = chunksize
        
        self.__bricks = {}

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

    def reset(self):
        for key,brick in self.items():
            brick.reset()
    
    def next(self):
        #loop over all loaded files
        self.logger.debug("Getting next chunk of data")
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

                    
    def get_handler(self,key):
        brick = self.__bricks[key]
        return brick.get_handler()
    
    def __getitem__(self,key):
        brick = self.__bricks[key]
        if any([brick.get_df() is None for brick in self.__bricks.values()]):
            self.logger.info(f"Retrieving initial dataframes for the first time")
            _ = [
                brick.get_chunk(self.chunksize)
                for brick in self.__bricks.values()
            ]
    
        df = brick.get_df()
        self.logger.debug(f"Got brick {brick}")
        return df
                
    def __setitem__(self,key,obj):
        if not isinstance(obj,DataBrick):
            raise NotImplementedError("When using InputData, the object must be of type "
                                      f"{DataBrick}")
        self.logger.info(f"Registering  {key} [{type(obj)}]")
        self.__bricks[key] = obj
        
    
    

