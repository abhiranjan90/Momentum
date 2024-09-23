import Utils
import pandas as pd
import yfinance as yf
import requests
import json
import os



class MarketUniverse:
    def __init__(self) -> None:
        pass
    

    def queryNseUrl(self, url):
        baseurl = "https://www.nseindia.com/"
        url = url
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                            'like Gecko) '
                            'Chrome/80.0.3987.149 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8', 'accept-encoding': 'gzip, deflate, br'}
        session = requests.Session()
        request = session.get(baseurl, headers=headers, timeout=5)
        cookies = dict(request.cookies)
        response = session.get(url, headers=headers, timeout=5, cookies=cookies)
        return response
    
    def get_All_Symbols(self, request_url):
        count = 0
        response = None
        while count < 3:
            try:
                response =  self.queryNseUrl(request_url)
                if response.status_code == 200:
                    break
                else:
                    count = count+1
                    continue
            except Exception as e:
                print("An exception occured...Tryin again")
                count = count +1
                continue
        
        return response

    def optionBackedEquityUniverse(self):
        pp = self.get_All_Symbols("https://www.nseindia.com/api/underlying-information")
        symbolNameToUnderlierDescription = {'Symbol' : [], 'Description' : []}
        if pp == None:
            return []
        else:
            if pp.status_code == 200:
                underlierListForEquityContracts = (pp.json()['data']['UnderlyingList'])
                for i in range (len(underlierListForEquityContracts)):
                    symbolNameToUnderlierDescription['Symbol'].append(underlierListForEquityContracts[i]['symbol'])
                    symbolNameToUnderlierDescription['Description'].append(underlierListForEquityContracts[i]['underlying'])


        return Utils.create_Dataframe_From_PyDictionary_ListValues(symbolNameToUnderlierDescription)
    
    def universe2(self):
        pass
