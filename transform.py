from pkgutil import extend_path
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

            #appending id, name and english name
            region_data.extend((region['id'], region['name'], region['name_en']))
            #appending country info
            region_data.extend((region['country']['name'], region['country']['code']))

            #append low to data frame
            regions_data.append(region_data)

        cols = ['id', 'name', 'name_english', 'country_name', 'country_code']
        df = pd.DataFrame(regions_data, columns=cols)

        return df



    def create_grape_table(self):
        '''
        Function to create the regions table from raw data
        '''

        raw_data = self.load_file('grape.json')

        grapes_data = []
        for grape in raw_data['grapes']:
            grape_data = []

            #append grape id, name, seo name
            grape_data.extend((
                grape['id'],
                grape['name'],
                grape['seo_name']
            ))

            grapes_data.append(grape_data)

        cols = ['id', 'name', 'seo_name']
        df = pd.DataFrame(grapes_data, columns=cols)

        return df
        

    
    def create_country_table(self):
        '''
        Function to create the countries table from raw data
        '''
        raw_data = self.load_file('country.json')

        countries_data = []
        for country in raw_data['countries']:
            country_data = []

            country_data.extend((
                country.get('code', None),
                country.get('name', None),
                country.get('seo_name', None),
                country['currency'].get('code', None),
                country['currency'].get('name', None),
            ))

            countries_data.append(country_data)
        cols = ['country_code', 'country_name', 'country_seo_name', 'country_curr_code', 'country_curr_name']
        df = pd.DataFrame(countries_data, columns=cols)

        return df


    #TODO
    def create_wine_table(self):
        pass
    def create_winery_table(self):
        pass



    def create_vintage_table(self):
        '''
        Function to create the vintage table from raw data
        '''

        raw_data = self.load_file('vintage.json')

        vintages_data = []
        for vintage in raw_data['vintages']:
            vintage_data = []
            #getting id, name and seo_name
            vintage_data.extend((
                            vintage['vintage'].get('id', None),                                   #vintage id
                            vintage['vintage']['name'].replace('\t', 't'),                        #vintage name
                            vintage['vintage'].get('seo_name', None),                             #vintage search engine name
                            vintage['vintage'].get('year', None),                                 #vintage year
                            vintage['vintage']['statistics'].get('ratings_average', None),        #avergae rating
                            vintage['vintage']['statistics'].get('ratings_count', None),          #number of ratings
                            vintage['vintage']['wine'].get('id', None)                            #wine id
                            ))

            vintages_data.append(vintage_data)

        cols = ['id', 'name', 'seo_name', 'year', 'avg_rating', 'rating_count',  'wine_id']
        df = pd.DataFrame(vintages_data, columns=cols)

        return df



    def create_table(self, table_name):
        if table_name == 'region':
            df = self.create_region_table()
        elif table_name == 'grape':
            df = self.create_grape_table()
        elif table_name == 'vintage':
            df = self.create_vintage_table()

        return df





















if __name__ == '__main__':
    d = Transform().create_table('region')
    PostGresClient().insert_data_from_df(d, 'region')
    
   