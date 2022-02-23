from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from coconnect.io.common import DataCollection,DataBrick
from sqlalchemy import inspect
import pandas as pd

class SqlDataCollection(DataCollection):
    def __init__(self,connection_string,chunksize=None,nrows=None,**kwargs):
        super().__init__(chunksize=chunksize)

        engine = create_engine(connection_string)
        if not database_exists(engine.url):
            create_database(engine.url)

        #get the names of existing tables
        self.logger.info(engine)
        self.engine = engine
        self.build()

    def reset(self):
        self.build()
        self.__df = None
        self.__end = False
        return True
        
    def build(self):
        insp  = inspect(self.engine)
        chunksize = self.chunksize
        if chunksize == None:
            chunksize = 1e6
            
        for table in insp.get_table_names():
            df_handler = pd.read_sql(table,self.engine,chunksize=chunksize)
            b = DataBrick(df_handler,name=table)

            #if table in self.keys():
            #    del self[table]
                
            self[table] = b
            
    def write(self,name,df,mode='w'):
        #set the method of pandas based on the mode supplied

        if mode == 'w':
            mode = 'r'
        
        if mode == 'a':
            if_exists = 'append'
        elif mode == 'r':
            if_exists = 'replace'
        elif mode == 'w':
            if_exists = 'fail'
        else:
            raise Exception(f"Unknown mode for dumping to sql, mode = '{mode}'")

        #check if the table exists already
        table_exists = name in self.existing_tables

        #index the dataframe
        pk = df.columns[0]
        df.set_index(pk,inplace=True)
        self.logger.info(f'updating {name} in {self.engine}')
        
        #check if the table already exists in the psql database
        if table_exists and False:
            #get the last row
            last_row_existing = pd.read_sql(f"select {pk} from {name} "
                                            f"order by {pk} desc limit 1",
                                                self.engine)
            
            #if there's already a row and the mode is set to append
            if len(last_row_existing) > 0 and mode == 'a':
                #get the cell value of the (this will be the id, e.g. condition_occurrence_id)
                last_pk_existing = last_row_existing.iloc[0,0]
                #get the index integer of this current dataframe
                first_pk_new = df.index[0]
                #workout and increase the indexing so the indexes are new
                index_diff = last_pk_existing - first_pk_new
                if index_diff >= 0:
                    self.logger.info("increasing index as already exists in psql")
                    df.index += index_diff + 1
                    
        #dump to sql
        df.to_sql(name, self.engine,if_exists=if_exists) 

        self.logger.info("finished save to psql")

