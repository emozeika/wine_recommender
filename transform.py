import pandas as pd
import json
import pprint as pprint


#loading config variables
from config import RAW_DATA_PATH, REPO_PATH


from postgres import PostGresClient


class Transform(object):
    RAW_PATH_ = RAW_DATA_PATH
    REPO_PATH_ = REPO_PATH



    def __init__(self):
        pass

    def load_file(self, file_name):
        '''
        Function to load raw json file
        '''

        file = open(self.RAW_PATH_ + '/' + file_name)
        raw_data = json.load(file)
        file.close()

        return raw_data


    def create_region_table(self):
        '''
        Function to create the regions table from raw data
        '''

        raw_data = self.load_file('region.json')

        regions_data = []
        for region in raw_data['regions']:
            region_data = []
            #appending name and english name
            region_data.extend((region['name'], region['name_en']))
    

            #append low to data frame
            regions_data.append(region_data)


        cols = ['name', 'name_english']
        df = pd.DataFrame(regions_data, columns=cols)

        return df









if __name__ == '__main__':
    d = Transform().create_region_table()
    PostGresClient().insert_data_from_df(d, 'region')
    
   