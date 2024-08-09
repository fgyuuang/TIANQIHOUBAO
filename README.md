# 天气后报网站天气数据爬取
## 1. 爬取目标
本代码通过爬取 [目标网址](http://www.tianqihoubao.com)，获得全国各城市历史天气数据。默认从2011.1开始到现在（2024.8），一个城市预计达到4745条数据。
## 2.功能支持列表

* [x] 爬取指定省份的所有城市
* [x] 使用线程池自定义控制爬取评论速度
* [x] 保存为csv文件`省份名/城市.csv`
* [x] ...
## 3.快速开始
#### 1.克隆到本地
```bash
 git clone https://github.com/fgyuuang/TIANQIHOUBAO.git
```
#### 2.进入目录

```bash
cd TIANQIHOUBAO
```
#### 3.安装依赖
```bash
pip install -r requirements.txt
```
#### 5.修改配置文件
AREAS修改为你需要爬取的省份。
#### 4.运行 `get_city.py`得到 {省份：{城市：url}}字典
```bash
python get_city.py
```
#### 5.运行 `tianqihoubao.py`得到天气数据
```bash
python tianqihoubao.py
```