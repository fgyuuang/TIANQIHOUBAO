import io
import os
import sys
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import csv
import time
import urllib
from utils.fake_user_agent import get_fake_user_agent
from utils.proxy import my_get_proxy
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='gbk')  # 改变标准输出的默认编码, 防止控制台打印乱码
from datetime import datetime
from config import AREAS
from concurrent.futures import ThreadPoolExecutor, as_completed
import DrissionPage
now_year = datetime.now().year
now_month = datetime.now().month
page=DrissionPage.SessionPage()
def DealsaveTocsv(df, fileName):
     '''
     将天气数据保存至csv文件
     '''
     df[['白天', '晚上']] = df['tq'].str.split('/', expand=True)

     # 分裂 'temp' 列
     df[['低温', '高温']] = df['temp'].str.split('/', expand=True)

     # 分裂 'wind' 列
     df[['白天风向', '晚上风向']] = df['wind'].str.split('/', expand=True)

     # 删除原有的列
     df.drop(['tq', 'temp', 'wind'], axis=1, inplace=True)
     df['日期'] = pd.to_datetime(df['日期'], format='%Y年%m月%d日')

     # 按照日期进行排序
     df.sort_values(by='日期', inplace=True)

     # 重置索引
     df.reset_index(drop=True, inplace=True)

     df.to_csv(fileName, index=False, encoding='gbk')

     print('Save all weather success!')


def get_data(city,year,month):
    url = 'http://www.tianqihoubao.com' + city + '/' + 'month' + '/' + str(year) + str(month) + '.html'
    while True:
        try:
            # print('requesting...', url)
            page.get(url,timeout=20, headers={'User-Agent': get_fake_user_agent('pc')})
            r=page.html
            # r.raise_for_status()  # 若请求不成功,抛出HTTPError 异常
            # r.encoding = 'gbk'
            print(f'{url}Request Success')
            soup = BeautifulSoup(r, 'lxml')
            all_weather = soup.find('div', class_="wdetail").find('table').find_all("tr")

            data = list()
            for tr in all_weather[1:]:
                td_li = tr.find_all("td")
                for td in td_li:
                    s = td.get_text()
                    data.append("".join(s.split()))

            res = np.array(data).reshape(-1, 4)
            result_weather = pd.DataFrame(res, columns=['日期', 'tq', 'temp', 'wind'])
            return result_weather
        except:
            print('error')


def get_city():
    url='http://www.tianqihoubao.com/lishi/'
    # print('Requesting...')
    r = requests.get(url, timeout=30)
    r.raise_for_status()  # 若请求不成功,抛出HTTPError 异常
    r.encoding = 'gbk'
    # print('Request Success')
    soup = BeautifulSoup(r.text, 'lxml')
    city_list= soup.find('div', class_="citychk").find_all('dl')
    citydic={}
    for dl in city_list:
        province_text= dl.find('dt').find('a').get_text()
        # print(province_text)
        dd = dl.find('dd').find_all('a')
        wherelist={}
        for a in dd:
            name = a.get_text()
            # print(name)
            url= a.get('href').split('.')[0]
            wherelist[name]=url
        citydic[province_text]=wherelist
    return citydic


if __name__ == '__main__':
    year_list = list(range(2011, now_year + 1))
    month_list = list(range(1, 13))
    citydic = get_city()
    threadPool = ThreadPoolExecutor(max_workers=5000)
    thread_list = []
    for area in AREAS:
        filedic = area + '/'
        if not os.path.exists(filedic):
            os.makedirs(filedic)
        for city,url in citydic[area].items():
            filename= filedic+city+'.csv'
            citydata=pd.DataFrame()
            for year in year_list:
                 for month in month_list:
                     if year == now_year and month > now_month:
                         break
                     month = str(month).zfill(2)
                     thread = threadPool.submit(get_data,url,year,month)
                     thread_list.append(thread)
            for thread in as_completed(thread_list):
                citydata =pd.concat([citydata,thread.result()],axis=0)
            DealsaveTocsv(citydata,filename)
    print('All Done')

