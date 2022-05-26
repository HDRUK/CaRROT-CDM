import pandas as pd
from carrot.tools.logger import Logger
from types import GeneratorType
import io

class DataCollection(Logger):
    def __init__(self,chunksize=None,nrows=None,**kwargs):
        self.logger.info("DataCollection Object Created")
        self.__bricks = {}
        self.chunksize = chunksize
        self.nrows = nrows
        
        if self.chunksize is not None:
            self.logger.info(f"Using a chunksize of '{self.chunksize}' nrows")

    def print(self):
        print (self.all())
        
    def all(self):
        return {
            key:self[key]
            for key in self.keys()
        }
    def finalise(self):
        pass
        
    def keys(self):
        return self.__bricks.keys()

    def items(self):
        return self.__bricks.items()

    def __setitem__(self,key,obj):
        self.logger.info(f"Registering  {key} [{obj}]")
        self.__bricks[key] = obj

    def load_global_ids(self):
        return

    def load_indexing(self):
        return
            
    def next(self):
        #loop over all loaded files
        self.logger.info("Getting next chunk of data")

        used_bricks = []
        for key,brick in self.items():
            if brick.is_finished():
                continue
            
            if brick.is_init():
                used_bricks.append(brick)
            else:
                continue
                
            self.logger.info(f"Getting the next chunk of size '{self.chunksize}' for '{key}'")
            brick.get_chunk(self.chunksize)
            n = len(brick.get_df())
            self.logger.info(f"--> Got {n} rows")
            if n == 0:
                brick.set_finished(True)
            
        #check if all __dataframe objects are empty
        #if they are, raise a StopIteration as processing has finished
        if all([x.is_finished() for x in used_bricks]):
            self.logger.info("All input files for this object have now been used.")
            raise StopIteration
        
                           
    def get_handler(self,key):
        brick = self.__bricks[key]
        return brick.get_handler()

    def get_all(self):
        self.logger.info(f"Retrieving initial dataframes for the first time")
        for b in self.__bricks.values():
            b.get_chunk(self.chunksize)
            b.set_init(True)

    def get(self,key):
        return self.__bricks[key]
            
    def __getitem__(self,key):
        brick = self.__bricks[key]
        if not brick.is_init():
            self.logger.info(f"Retrieving initial dataframe for '{key}' for the first time")
            brick.get_chunk(self.chunksize)
            brick.set_init(True)
            
        #if any(not x.is_init() for x in self.__bricks.values()):
        #    self.get_all()
                
        df = brick.get_df()
        self.logger.debug(f"Got brick {brick}")
        return df
    
    def reset(self):
        self.logger.info(f"resetting used bricks")
        for key,brick in self.items():
            brick.reset()

class DataBrick:
    def __init__(self,df_handler,name=None):
        self.name = name
        self.__df_handler = df_handler
        self.__df = None
        self.__end = False
        self.__is_init = False

    def get_handler(self):
        return self.__df_handler

    def is_finished(self):
        return self.__end
    
    def set_finished(self,value):
        self.__end = value

    def is_init(self):
        return self.__is_init

    def set_init(self,value):
        self.__is_init = value
    
    def reset(self):
        if isinstance(self.__df_handler,pd.io.parsers.TextFileReader):
            options = self.__df_handler.orig_options
            f = self.__df_handler.f
            del self.__df_handler
            options['engine'] = 'c'
            if isinstance(f,io.StringIO):
                f.seek(0)
            self.__df_handler = pd.io.parsers.TextFileReader(f,**options)
            
        self.__df = None
        self.__end = False
        self.__is_init = False
        return True
    
    def get_chunk(self,chunksize):
        if self.__end == True:
            return
        #if the df handler is a TextFileReader, get a dataframe chunk
        if isinstance(self.__df_handler,pd.io.parsers.TextFileReader):
            try:
                #for this file reader, get the next chunk of data
                self.__df = self.__df_handler.get_chunk(chunksize)
            except StopIteration:#,ValueError):
                #otherwise, if at the end of the file reader, return an empty frame
                self.__df = pd.DataFrame(columns=self.__df.columns) if self.__df is not None else None
                self.__end = True
        elif isinstance(self.__df_handler,pd.DataFrame):
            #if we're handling non-chunked data
            if self.__df is not None:
                #return an empty dataframe if we've already loaded this dataframe
                self.__df = pd.DataFrame(columns=self.__df.columns)
            else:
                #otherwise return the dataframe as it's the first time we're getting it
                self.__df = self.__df_handler
            self.__end = True
        elif isinstance(self.__df_handler, GeneratorType):
            try:
                self.__df = next(self.__df_handler)
            except StopIteration:
                self.__df = pd.DataFrame(columns=self.__df.columns) if self.__df is not None else None
                self.__end = True
        else:
            raise NotImplementedError(f"{type(self.__df_handler)} not implemented")

    def get_df(self):
        return self.__df
