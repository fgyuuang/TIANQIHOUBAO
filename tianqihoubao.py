import io
import json
import os
import queue
import sys
import threading
import pandas as pd
import pickle
import requests
from bs4 import BeautifulSoup
from fake_user_agent import get_fake_user_agent
from datetime import datetime
from config import AREAS,TIME_OUT,DOWNLOAD_NUMBER,SAVE_NUMBER,IS_OVER
import DrissionPage
import logging
from threading import Thread,Lock
now_year = datetime.now().year
now_month = datetime.now().month
page=DrissionPage.SessionPage()
starttime = datetime.now()
# 设置日志记录器
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# 创建一个适配器并设置更大的连接池大小
adapter = HTTPAdapter(
    pool_connections=50,  # 最大连接数
    pool_maxsize=50,      # 最大连接池大小
    max_retries=Retry(total=3, backoff_factor=0.1)  # 设置重试机制
)

# 创建一个会话并挂载适配器
session = requests.Session()
session.mount('http://', adapter)
session.mount('https://', adapter)
def DealsaveTocsv(df, fileName):
     '''
     将天气数据保存至csv文件
     '''
     df[['白天', '晚上']] = df['天气状况'].str.split('/', expand=True)

     # 分裂 'temp' 列
     df[['低温', '高温']] = df['最低气温/最高气温'].str.split('/', expand=True)

     # 分裂 'wind' 列
     df[['白天风向', '晚上风向']] = df['风力风向(夜间/白天)'].str.split('/', expand=True)

     # 删除原有的列
     df.drop(['天气状况', '最低气温/最高气温', '风力风向(夜间/白天)'], axis=1, inplace=True)
     df['日期'] = pd.to_datetime(df['日期'], format='%Y年%m月%d日')

     # 按照日期进行排序
     df.sort_values(by='日期', inplace=True)

     # 重置索引
     df.reset_index(drop=True, inplace=True)

     df.to_csv(fileName, index=False, encoding='gbk')

     # logger.info('Save all weather success!')
def get_city():
    """
    从天气网站获取城市列表，并将其整理成字典形式。

    Returns:
        dict: 包含省份及其城市的字典。
    """
    url = 'http://www.tianqihoubao.com/lishi/'
    while True:
        try:
            # 发送 HTTP 请求获取网页内容
            page.get(url, timeout=TIME_OUT)
            # 解析网页内容
            city_list = page.ele('xpath=//div[@class="citychk"]').eles('xpath=//dl')
            citydic = {}
            for dl in city_list:
                province_text = dl.ele('xpath=//dt').text
                dd = dl.ele('xpath=//dd').eles('xpath=//a')
                wherelist = {}
                for a in dd:
                    name = a.text
                    href = a.href
                    url = href.split('/')[-1].split('.')[0]
                    wherelist[name] = [url, href]
                    citydic[province_text] = wherelist

            return citydic

        except requests.RequestException as e:
            logger.error(f'HTTP 请求错误: {e}')
        except Exception as e:
            logger.error(f'解析错误: {e}')

def get_data(city,year,month):
    url = f'http://www.tianqihoubao.com/lishi/{city}/month/{year}{month}.html'
    # logger.info(f'开始爬取{url}')
    while True:
        try:
            page.get(url,timeout=TIME_OUT, headers={'User-Agent': get_fake_user_agent('pc')})
            list = page.eles('xpath=//tr')
            datarows = []
            for li in list:
                a = li.s_eles('tag:td')
                l = [i.text for i in a]
                datarows.append(l)
            columns = datarows[0]
            # 提取数据行（剩余的元素）
            data_values = datarows[1:]
            # 创建DataFrame
            result_weather = pd.DataFrame(data_values, columns=columns)
            return result_weather
        except Exception as e:
            logger.error(f'解析或数据处理错误: {e},错误网址为{url}',)
def do_craw(urlqueue:queue.Queue,parsequeue: queue.Queue):
    while True:
        # logger.info(f'进程{threading.current_thread().name}开始')
        url,year,month,filename = urlqueue.get()
        # logger.info(f'进程{threading.current_thread().name}开始获得{url,month,year,filename}')
        data=get_data(url,month,year)
        parsequeue.put((data,filename))
        logger.info(f'{threading.current_thread().name}爬取成功{url}crwal size:{urlqueue.qsize()}')
        if urlqueue.qsize() == 0:
            end_time=datetime.now()
            logger.info(f'urlqueue为空，关闭线程,用时{end_time-starttime}')
            return
def do_save(parsequeue: queue.Queue,urlqueue:queue.Queue):
    while True:
        data,filename = parsequeue.get()
        DealsaveTocsv(data,filename)
        logger.info( f'{threading.current_thread().name} 保存成功{file} save size:{parsequeue.qsize()}')
        if parsequeue.qsize() == 0 and urlqueue.qsize() == 0:
            end_time=datetime.now()
            logger.info(f'urlqueue与parsequeue为空，关闭线程,用时{end_time-starttime}',)
            return
if __name__ == '__main__':
    year_list = list(range(2011, now_year + 1))
    month_list = list(range(1, 13))
    urlqueue = queue.Queue()
    parsequeue = queue.Queue()
    # citydic = get_city()
    if not os.path.exists('citydata.json'):
        citydic = get_city()
        json_string = json.dumps(citydic, indent=4)
        with open('citydata.json', 'w', encoding='utf-8') as file:
            json.dump(citydic, file, ensure_ascii=False, indent=4)
    # 读取变量
    else:
        logger.info('citydata.json exists')
        with open('citydata.json', 'r',encoding='utf-8') as file:
            citydic = json.load(file)
    # logger.info(f'城市列表读取成功{citydic}')
    for area in AREAS:
        filedic = area + '/'
        if not os.path.exists(filedic):
            os.makedirs(filedic)
        for city,[url,realurl] in citydic[area].items():
            filename= filedic+city+'/'
            if not os.path.exists(filename) or IS_OVER:
                if not os.path.exists(filename):
                    os.makedirs(filename)
                for year in year_list:
                     for month in month_list:
                         if year == now_year and month > now_month:
                             break
                         month = str(month).zfill(2)
                         name= filename+f'{year}{month}.csv'
                         urlqueue.put((url,month,year,name))
                         # logger.info(f'添加url:{(url,month,year,name)}到队列')

            else:
                logger.info(f'{filename}exists')

    # print('开始爬取',urlqueue)
    for idx in range(DOWNLOAD_NUMBER):
        t = Thread(target=do_craw, args=(urlqueue, parsequeue), name=f'爬取线程{idx}')
        t.start()
    for idx in range(SAVE_NUMBER):
        t = Thread(target=do_save, args=(parsequeue,urlqueue), name=f'保存线程{idx}')
        t.start()
    endtime= datetime.now()
    logger.info('All Done,time is %s' % (endtime - starttime))

