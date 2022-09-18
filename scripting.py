
import requests


class Vivino(object):
    BASEURL_ = 'https://www.vivino.com/api/'
    REGIONS_ = '/regions'

    def __init__(self):
        pass

    def get_regions(self):
        url = self.BASEURL_  + self.REGIONS_
        requests.get(url)




if __name__  == '__main__':
    print(Vivino().BASEURL_)