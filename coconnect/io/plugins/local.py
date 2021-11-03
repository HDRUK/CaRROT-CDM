import pandas as pd
from coconnect.tools.logger import Logger

class InputData:
    def __init__(self,chunksize):
        self.chunksize = chunksize

        self.__file_readers = {}
        self.__dataframe = {}

        self.logger = Logger(self.__class__.__name__)
        self.logger.info("InputData Object Created")
        if self.chunksize is not None:
            self.logger.info(f"Using a chunksize of '{self.chunksize}' nrows")

    def all(self):
        return {
            key:self[key]
            for key in self.keys()
        }
        
    def keys(self):
        return self.__file_readers.keys()

    def next(self):
        #loop over all loaded files
        for key in self.keys():
            #get the next dataframe chunk for this file
            self.__dataframe[key] = self.get_df_chunk(key)

        #check if all __dataframe objects are empty
        #if they are, reaise a StopIteration as processing has finished
        if all([x.empty for x in self.__dataframe.values()]):
            self.logger.debug("All input files have now been processed.")
            raise StopIteration
        
        self.logger.info(f"Moving onto the next chunk of data (of size {self.chunksize})")

        
    def get_df_chunk(self,key):
        #obtain the file by key
        obj = self.__file_readers[key]
        #if it is a TextFileReader, get a dataframe chunk
        if isinstance(obj,pd.io.parsers.TextFileReader):
            try:
                #for this file reader, get the next chunk of data and update self.__dataframe
                return obj.get_chunk(self.chunksize)
            except StopIteration:
                #otherwise, if at the end of the file reader, return an empty frame
                return pd.DataFrame(columns=self.__dataframe[key].columns)
        else:
            #if we're handling non-chunked data
            #return an empty dataframe if we've already loaded this dataframe
            if key in self.__dataframe.keys():
                return pd.DataFrame()
            #otherwise return the dataframe as it's the first time we're getting it
            return obj
            

    def __getitem__(self,key):
        if key not in self.__dataframe.keys():
            self.__dataframe[key] = self.get_df_chunk(key)
        return self.__dataframe[key]
        
    def __setitem__(self,key,obj):
        if not (isinstance(obj,pd.DataFrame) or isinstance(obj,pd.io.parsers.TextFileReader)):
            raise NotImplementedError("When using InputData, the object must be of type "
                                      f"{pd.DataFrame} or {pd.io.parsers.TextFileReader} ")
        self.logger.info(f"Registering  {key} [{type(obj)}]")
        self.__file_readers[key] = obj
