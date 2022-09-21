from vivino import Vivino
from transform import Transform 
from postgres import PostGresClient


#class to run entire ETL pipeline

class Pipeline(object):
    ETL_ = {
        'country' : ['country'],
        'region' : ['region'],
        'grape' : ['grape']#,
        #'style' : ['style'],
        #'vintage' : ['vintage', 'wine', 'wineries']
    }

    def __init__(self):
        pass

    
    def extract(self, endpoint):
        Vivino().save_data(endpoint)

    def transform(self, table):
        table =  Transform().create_table(table)
        return table

    def load(self, df, table):
        PostGresClient().insert_data_from_df(df, table)

    def run(self):
        for endpoint in self.ETL_.keys():
            print(f'---------- Pulling raw data from endpoint: {endpoint} ----------')
            self.extract(endpoint)
            for table in self.ETL_[endpoint]:
                print(f'---------- Building table: {table} to load into database ----------')
                df = self.transform(table)
                print(f'---------- Loading table {table} into database ----------')
                self.load(df, table)
                print(f'---------- ETL pipeline has been complete for table: {table} ----------')

    

