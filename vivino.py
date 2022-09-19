#packages that are needed for functions inside Vivino class
from pprint import pprint
from urllib import response
import requests
import json


#loading in necessary variables from config file
from config import RAW_DATA_PATH



class Vivino(object):
    '''
    This is a class to extract data from Vivino.com wine library
    '''

    BASEURL_ = 'https://www.vivino.com/api/'
    ENDPOINT_ = {
        'region' : '/regions',
        'style' : '/wine_styles',
        'country' : '/countries',
        'grape' : '/grapes',
        'vintage' : '/explore/explore?price_range_min=1&page={}&per_page=50'   
    }
    HEADERS_ =  { 
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
    }

    def __init__(self):
        pass



    def get_vintage_data(self):
        #need to add vintages to the original data in a loop since it will not 
        #allow a straight pull

        #start with an initial data pull
        url = self.BASEURL_ + self.ENDPOINT_['vintage'].format(1)
        response = requests.get(url, headers=self.HEADERS_).json()
        response.pop('records', None)

        #finding the numbeer of loops that need to be made
        num_pages = response['explore_vintage']['records_matched'] 
        print(num_pages)
        for i in range(2,5): #change this after testing
            #making new request
            url = self.BASEURL_ + self.ENDPOINT_['vintage'].format(i)
            new_matches = requests.get(url, headers=self.HEADERS_).json()['explore_vintage']['matches']

            #adding matches to data
            response['explore_vintage']['matches'] += new_matches


        return response


    
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
    
    r = Vivino().get_data('vintage')
    print(len(r['explore_vintage']['matches']))

