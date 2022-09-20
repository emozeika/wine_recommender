#packages that are needed for functions inside Vivino class
from pprint import pprint
from urllib import response
import requests
import json
import math

#loading in necessary variables from config file
from config import RAW_DATA_PATH



class Vivino(object):
    '''
    This is a class to extract data from Vivino.com wine library
    '''

    BASEURL_ = 'https://www.vivino.com/api'
    ENDPOINT_ = {
        'region' : '/regions',
        'style' : '/wine_styles',
        'country' : '/countries',
        'grape' : '/grapes',
        'vintage': '/explore/explore?region_ids[]={}&page={}&per_page=50'
        #'vintage' : '/explore/explore?price_range_min=1&page={}&per_page=50'   
    }
    HEADERS_ =  { 
        #"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
    }

    def __init__(self):
        pass



    def get_vintage_data_old(self):
        #need to add vintages to the original data in a loop since it will not 
        #allow a straight pull

        #start with an initial data pull
        url = self.BASEURL_ + self.ENDPOINT_['vintage'].format(1)
        response = requests.get(url, headers=self.HEADERS_).json()
        response.pop('records', None)

        #finding the numbeer of loops that need to be made
        num_pages = response['explore_vintage']['records_matched'] //50 + 1
        print(num_pages)
        for i in range(2,num_pages): 
            print(i)
            #making new request
            url = self.BASEURL_ + self.ENDPOINT_['vintage'].format(i)
            new_matches = requests.get(url, headers=self.HEADERS_).json()['explore_vintage']['matches']

            #adding matches to data
            response['explore_vintage']['matches'] += new_matches


        return response

    
    def get_vintage_data(self):

        #get regions and ids because we have to loop through there
        url = self.BASEURL_ + self.ENDPOINT_['region'] 
        response = requests.get(url, headers=self.HEADERS_).json()

        #making a list of region ids to loop through
        region_ids = []
        for region in response['regions']:
            id = region['id']
            region_ids.append(region['id'])

        #initializing data 
        data = {'vintages' : []}

        for num, id in enumerate(region_ids):
            if num % 100 == 0: print(f'Region {num} of {len(region_ids)}')

            #need to run a check first to get the number of matches
            url = self.BASEURL_ + self.ENDPOINT_['vintage'].format(id, 1)
            response = requests.get(url, headers=self.HEADERS_).json()
            num_matches = response['explore_vintage']['records_matched']

            #if statement if we can pull all the records in 1 request else we loop through pages
            if num_matches <= 50:
                data['vintages'] += response['explore_vintage']['matches']  
            else:
                for i in range(1, math.ceil(num_matches/50)+1):
                    url = self.BASEURL_ + self.ENDPOINT_['vintage'].format(id, i)
                    response = requests.get(url, headers=self.HEADERS_).json()
                    data['vintages'] += response['explore_vintage']['matches']
            
        
        return data


    
    def get_data(self, data_type):
        
        if data_type == 'vintage':
            return self.get_vintage_data()

        url = self.BASEURL_ + self.ENDPOINT_[data_type] 
        response = requests.get(url, headers=self.HEADERS_).json()

        return response 



    def save_data(self, data_type):
        response = self.get_data(data_type)
        
        if response:
            with open(f'{RAW_DATA_PATH}/{data_type}.json', 'w') as outputfile:
                json.dump(response, outputfile)


 


if __name__ == '__main__':
    
    Vivino().save_data('vintage')
    #print(len(r['explore_vintage']['matches']))

