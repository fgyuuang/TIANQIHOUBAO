import io
import os
import queue
import sys
import threading
import pandas as pd
import pickle
from get_city import get_city
from fake_user_agent import get_fake_user_agent
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='gbk')  # 改变标准输出的默认编码, 防止控制台打印乱码
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

     logger.info('Save all weather success!')


def get_data(city,year,month):
    url = f'http://www.tianqihoubao.com{city}/month{year}{month}.html'
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
            logger.error(f'解析或数据处理错误: {e}')
def do_craw(parsequeue: queue.Queue,urlqueue:queue.Queue):
    while True:
        url,month,year,filename = urlqueue.get()
        data=get_data(url,month,year)
        parsequeue.put((data,filename))
        print(threading.current_thread().name, '爬取成功', url, f'crwal size:{urlqueue.qsize()}')
        if urlqueue.qsize() == 0:
            end_time=datetime.now()
            print('urlqueue为空，关闭线程,用时',end_time-starttime)
            return
def do_save(parsequeue: queue.Queue):
    while True:
        data,filename = parsequeue.get()
        DealsaveTocsv(data,filename)
        print(threading.current_thread().name, '保存成功', file, f'save size:{parsequeue.qsize()}')

if __name__ == '__main__':
    year_list = list(range(2011, now_year + 1))
    month_list = list(range(1, 13))
    urlqueue = queue.Queue()
    parsequeue = queue.Queue()
    # 读取变量
    if not os.path.exists('citydata.pkl'):
        get_city()
    with open('citydata.pkl', 'rb') as file:
        citydic = pickle.load(file)
    logger.info(f'城市列表读取成功{citydic}')
    for area in AREAS:
        filedic = area + '/'
        if not os.path.exists(filedic):
            os.makedirs(filedic)
        for city,url in citydic[area].items():
            filename= filedic+city+'/'
            if not os.path.exists(filename) or IS_OVER:
                os.makedirs(filename)
                for year in year_list:
                     for month in month_list:
                         if year == now_year and month > now_month:
                             break
                         month = str(month).zfill(2)
                         name= filename+f'{year}{month}.csv'
                         urlqueue.put((url,month,year,name))
            else:
                logger.info(f'{filename}exists')
    for idx in range(DOWNLOAD_NUMBER):
        t = Thread(target=do_craw, args=(urlqueue, parsequeue), name=f'爬取线程{idx}')
        t.start()
    for idx in range(SAVE_NUMBER):
        t = Thread(target=do_save, args=(parsequeue,), name=f'保存线程{idx}')
        t.start()
    endtime= datetime.now()
    logger.info('All Done,time is %s' % (endtime - starttime))

