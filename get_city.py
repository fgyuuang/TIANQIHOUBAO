import logging
import os

import requests
from bs4 import BeautifulSoup
import pickle
from config import TIME_OUT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
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
            response = requests.get(url, timeout=TIME_OUT)
            response.raise_for_status()  # 检查请求是否成功

            # 解析网页内容
            soup = BeautifulSoup(response.text, 'lxml')
            city_list = soup.find('div', class_="citychk").find_all('dl')
            citydic = {}

            for dl in city_list:
                province_text = dl.find('dt').find('a').get_text()
                dd = dl.find('dd').find_all('a')
                wherelist = {}
                for a in dd:
                    name = a.get_text()
                    href = a.get('href')
                    url = href.split('.')[0]
                    wherelist[name] = url
                citydic[province_text] = wherelist

            return citydic

        except requests.RequestException as e:
            logger.error(f'HTTP 请求错误: {e}')
        except Exception as e:
            logger.error(f'解析错误: {e}')
# 保存变量
if __name__ == '__main__':
    if not os.path.exists('citydata.pkl'):
        data = get_city()
        with open('citydata.pkl', 'wb') as file:
            pickle.dump(data, file)
        print(data)


