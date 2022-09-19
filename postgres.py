from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import io


#import config variables
from config import  ( 
                        POSTGRES_HOSTNAME, 
                        POSTGRES_USERNAME, 
                        POSTGRES_PWD, 
                        POSTGRES_PORT, 
                        POSTGRES_DATABASE
                    )




class PostGresClient(object):
    HOSTNAME_ = POSTGRES_HOSTNAME
    USERNAME_ = POSTGRES_USERNAME
    PWD_ = POSTGRES_PWD
    PORT_ = POSTGRES_PORT
    DATABASE_ = POSTGRES_DATABASE

    def __init__(self):
        self.ENGINE_ = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
            self.USERNAME_,
            self.PWD_,
            self.HOSTNAME_,
            self.PORT_,
            self.DATABASE_
        ))

    def create_table_from_df(self, df, table_name):

        #drop old table and replace if exists
        df.head(0).to_sql(table_name, self.ENGINE_, if_exists='replace',index=False)

    
    def insert_data_from_df(self, df, table_name):
        
        #create new table or delete table if exists
        self.create_table_from_df(df, table_name)

        #create connection
        conn = self.ENGINE_.raw_connection()
        cur = conn.cursor()

        #create output text for connection
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        #add data to table
        cur.copy_from(output, table_name) 
        conn.commit()





if __name__ =='__main__':
    df = pd.read_csv('x60.csv')
    
    PostGresClient().insert_data_from_df(df, 'client_test')