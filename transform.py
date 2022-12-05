from operator import le
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

            region_data.extend((
                region.get('id', 'NULL') if region.get('id', None) else 'NULL',
                region.get('name', 'NULL') if region.get('name', None) else 'NULL',
                region.get('name_en', 'NULL') if region.get('name_en', None) else 'NULL',
                region.get('seo_name', 'NULL') if region.get('seo_name', None) else 'NULL',
                region.get('country').get('name', 'NULL') if region.get('country', None) else 'NULL',
                region.get('country').get('code', 'NULL') if region.get('country', None) else 'NULL',
            ))

            #append low to data frame
            regions_data.append(region_data)

        cols = ['id', 'name', 'name_english', 'seo_name', 'country_name', 'country_code']
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
                grape.get('id', 'NULL'),
                grape.get('name', 'NULL'),
                grape.get('seo_name', 'NULL')
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
                country.get('code', 'NULL'),
                country.get('name', 'NULL'),
                country.get('seo_name', 'NULL'),
                country.get('currency').get('code', 'NULL') if country.get('currency', None) else 'NULL',
                country.get('currency').get('name', 'NULL') if country.get('currency', None) else 'NULL'
            ))

            countries_data.append(country_data)
        cols = ['country_code', 'country_name', 'country_seo_name', 'country_curr_code', 'country_curr_name']
        df = pd.DataFrame(countries_data, columns=cols)

        return df



    def create_vintage_table(self):
        '''
        Function to create the vintage table from raw data
        '''

        raw_data = self.load_file('vintage.json')

        vintages_data = []
        for vintage in raw_data['vintages']:
            price = vintage.get('price', None)
            vintage = vintage['vintage']

            vintage_data = []
            #getting id, name and seo_name
            vintage_data.extend((
                vintage.get('id', 'NULL'),                                                              #vintage id
                vintage.get('name').replace('\t', ' ') if vintage.get('name', None) else 'NULL',        #vintage name
                vintage.get('seo_name', 'NULL'),                                                        #vintage search engine name
                vintage.get('year', 'NULL'),                                                            #vintage year
                vintage.get('statistics').get('ratings_average', 'NULL') if vintage.get('statistics', None) else 'NULL',        #avergae rating
                vintage.get('statistics').get('ratings_count', 'NULL') if vintage.get('statistics', None) else 'NULL',          #number of ratings
                vintage.get('wine').get('id', 'NULL') if vintage.get('wine', None) else 'NULL',                             #wine id
                price.get('amount', 'NULL') if price else 'NULL',
                price.get('bottle_type_id', 'NULL') if price else 'NULL'
            ))

            vintages_data.append(vintage_data)

        cols = ['id', 'name', 'seo_name', 'year', 'avg_rating', 'rating_count',  'wine_id', 'price', 'bottle_type_id']
        df = pd.DataFrame(vintages_data, columns=cols)

        return df
    

    
    def create_style_table(self):
        '''
        Function to create the style table from raw data
        '''
        raw_data = self.load_file('style.json')

        styles_data = []
        for style in raw_data['wine_styles']:
            style_data = []

            style_data.extend((
                style.get('id', 'NULL') if style.get('id', None) else 'NULL',
                style.get('name').replace('\t', ' ') if style.get('name', None) else 'NULL',
                style.get('seo_name', 'NULL') if style.get('seo_name', None) else 'NULL',
                style.get('description', 'NULL').replace('\n', '') if style.get('description', None) else 'NULL',
                style.get('body', 'NULL') if style.get('body', None) else 'NULL',
                style.get('body_description', 'NULL') if style.get('body_description', None) else 'NULL',
                style.get('acidity', 'NULL') if style.get('acidity', None) else 'NULL',
                style.get('acidity_description', 'NULL') if style.get('acidity_description', None) else 'NULL',
                style.get('country').get('code', 'NULL') if style.get('country', None) else 'NULL',
                style.get('wine_type_id', 'NULL') if style.get('wine_type_id', None) else 'NULL',
                style.get('region').get('id', 'NULL') if style.get('region', None) else 'NULL'
            ))

            styles_data.append(style_data)

        cols = ['id', 'name', 'seo_name', 'description', 'body', 'body_desc', 'acidity',
                'acidity_desc', 'country_code', 'wine_type_id', 'region_id']
        df = pd.DataFrame(styles_data, columns=cols)

        return df



    def create_wine_table(self):
        '''
        Function to create the wine table from raw data
        '''
        raw_data = self.load_file('vintage.json')

        wines_data = []
        for wine in raw_data['vintages']:
            wine = wine.get('vintage').get('wine', None)
            
            wine_data = []
            if wine:
                wine_data.extend((
                    wine.get('id', 'NULL'),
                    wine.get('name', 'NULL').replace('\t', ''),
                    wine.get('seo_name', 'NULL'),
                    wine.get('type_id', 'NULL'),
                    wine.get('region').get('id', 'NULL') if wine.get('region', None) else 'NULL',
                    wine.get('winery').get('id', 'NULL') if wine.get('winery', None) else 'NULL',
                    wine.get('taste').get('structure').get('acidity', 'NULL') if wine.get('taste').get('structure', None) else 'NULL',
                    wine.get('taste').get('structure').get('fizziness', 'NULL') if wine.get('taste').get('structure', None) else 'NULL',
                    wine.get('taste').get('structure').get('intensity', 'NULL') if wine.get('taste').get('structure', None) else 'NULL',
                    wine.get('taste').get('structure').get('sweetness', 'NULL') if wine.get('taste').get('structure', None) else 'NULL',
                    wine.get('taste').get('structure').get('tannin', 'NULL') if wine.get('taste').get('structure', None) else 'NULL',
                    #CAN POTENTIALLY ADD FLAVOR NOTES HERE!!!!!!!
                    wine.get('statistics').get('ratings_count', 'NULL') if wine.get('statistics', None) else 'NULL',
                    wine.get('statistics').get('ratings_average', 'NULL') if wine.get('statistics', None) else 'NULL',
                    wine.get('style').get('id', 'NULL') if wine.get('style', None) else 'NULL',

                ))
                wines_data.append(wine_data)

        cols = ['id', 'name', 'seo_name', 'wine_type_id', 'region_id', 'winery_id',
                'acidity', 'fizziness', 'sweetness', 'intensity', 'tannin', 'ratings_count', 'avg_rating',
                'style_id']
        df = df = pd.DataFrame(wines_data, columns=cols)
        df.drop_duplicates(inplace=True) #drop dupicates since we build through vintage
        return df



    def create_winery_table(self):
        '''
        Function to create the winery table from raw data
        '''
        raw_data = self.load_file('vintage.json')

        wineries_data = []
        for winery in raw_data['vintages']:
            winery = winery.get('vintage').get('wine').get('winery', None)
    
            winery_data = []
            if winery:
                winery_data.extend((
                    winery.get('id', 'NULL'),
                    winery.get('name', 'NULL'),
                    winery.get('seo_name', 'NULL')
                ))
                wineries_data.append(winery_data)

        cols = ['id', 'name', 'seo_name']
        df = df = pd.DataFrame(wineries_data, columns=cols)
        df.drop_duplicates(inplace=True) #drop dupicates since we build through vintage
        return df



    def create_wine_flavor_table(self):
        '''
        Function to create the winery flavor profile grouping from raw data
        '''
        raw_data = self.load_file('vintage.json')

        flavors_data = []
        for wine in raw_data['vintages']:
            wine_id = wine.get('vintage').get('wine').get('id', 'NULL')
            wine = wine.get('vintage').get('wine').get('taste', None)

            if wine:
                flavor = wine.get('flavor', None)
                if flavor:
                    for i in range(len(flavor)):
                        flavor_data = [wine_id, flavor[i].get('group', None)]
                        #getting keywords
                        p_kwds, s_kwds = '', ''
                        if flavor[i].get('primary_keywords', None):
                            for j in range(len(flavor[i].get('primary_keywords'))):
                                p_kwds += (flavor[i].get('primary_keywords')[j].get('name').replace(' ', '_') + '|')
                            flavor_data.append(p_kwds[:-1])
                        else:
                            flavor_data.append('NULL')
                        if flavor[i].get('secondary_keywords', None):
                            for j in range(len(flavor[i].get('secondary_keywords'))):
                                s_kwds += (flavor[i].get('secondary_keywords')[j].get('name').replace(' ', '_') + '|')
                            flavor_data.append(s_kwds[:-1])
                        else:
                            flavor_data.append('NULL')
                        flavors_data.append(flavor_data)
        


        cols = ['wine_id', 'group', 'primary_keywords', 'secondary_keywords']
        df = pd.DataFrame(flavors_data, columns=cols)
        df.drop_duplicates(subset=['wine_id', 'group'], inplace=True) #drop dupicates since we build through vintage
        return df



    def create_flavor_frequency_table(self):
        '''
        Function to create the winery flavor frequency profile grouping from raw data
        '''
        raw_data = self.load_file('vintage.json')
        flavors_data = []
        for wine in raw_data['vintages']:
            wine_id = wine.get('vintage').get('wine').get('id', 'NULL')
            wine = wine.get('vintage').get('wine').get('taste', None)

            if wine:
                flavor = wine.get('flavor', None)
                if flavor:
                    for i in range(len(flavor)):
                        prmy_keywrds = flavor[i].get('primary_keywords', None)
                        if prmy_keywrds:
                            for j in range(len(prmy_keywrds)):
                                flavor_data = [wine_id, flavor[i].get('group', None)]
                                flavor_data.extend((
                                    prmy_keywrds[j].get('id', 'NULL'),
                                    prmy_keywrds[j].get('name', 'NULL'),
                                    prmy_keywrds[j].get('count', 'NULL')
                                ))
                                flavors_data.append(flavor_data)

        cols = ['wine_id', 'group', 'keyword_id', 'keyword_name', 'keyword_count']
        df = pd.DataFrame(flavors_data, columns=cols)
        df.drop_duplicates(subset=['wine_id', 'group', 'keyword_id', 'keyword_count'], inplace=True) #drop dupicates since we build through vintage
        return df             



    def create_table(self, table_name):
        if table_name == 'region':
            df = self.create_region_table()
        elif table_name == 'grape':
            df = self.create_grape_table()
        elif table_name == 'vintage':
            df = self.create_vintage_table()
        elif table_name == 'country':
            df = self.create_country_table()
        elif table_name == 'style':
            df = self.create_style_table()
        elif table_name == 'wine':
            df = self.create_wine_table()
        elif table_name == 'winery':
            df = self.create_winery_table()
        elif table_name == 'flavor':
            df = self.create_wine_flavor_table()
        elif table_name == 'flavor_freq':
            df = self.create_flavor_frequency_table()
        return df



if __name__ == '__main__':
    d = Transform().create_table('flavor')
    PostGresClient().insert_data_from_df(d, 'flavor')
   
