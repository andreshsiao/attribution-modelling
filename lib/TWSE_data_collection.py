
import requests
import datetime
import datatable as dt
from sqlitedict import SqliteDict
import numpy as np

def get_stock_history(date, stock_no):
    quotes = []
    url = f'http://www.twse.com.tw/exchangeReport/STOCK_DAY?date={date}&stockNo={stock_no}'
    proxies = {'http': '10.43.2.22:8080', 'https': '10.43.2.22:8080'}
    r = requests.get(url, proxies=proxies)
    data = r.json()
    result = data['data']
    numpy_array = np.array(result)
    return numpy_array

if __name__ == '__main__':
    res = get_stock_history(20220728, 2330)
    print(res)
    print(dt.Frame(res))
