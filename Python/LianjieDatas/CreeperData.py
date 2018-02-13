# -*- coding: utf-8 -*-

import sqlite3
import re
import requests
import random
import threading
from bs4 import BeautifulSoup
import json

import sys
# reload(sys)
# sys.setdefaultencoding("utf-8")

#登录，不登录不能爬取三个月之内的数据
# import LianJiaLogIn


#Some User Agents
hds = [{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},
    {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},
    {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},
    {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
    {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
    {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},
    {'User-Agent': 'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},
    {'User-Agent': 'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

url = ''


#北京区域列表
regions = [u"东城",u"西城",u"朝阳",u"海淀",u"丰台",u"石景山","通州",u"昌平",u"大兴",u"亦庄开发区",u"顺义",u"房山",u"门头沟",u"平谷",u"怀柔",u"密云",u"延庆",u"燕郊"]

logPath ='log.txt'

lock = threading.Lock()


class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):
        self.lock = threading.RLock() #锁
        self.path = path #数据库连接参数

        if command != '':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)

    # 获取连接
    def get_conn(self):
        conn = sqlite3.connect(self.path)#,check_same_thread=False)
        conn.text_factory=str
        return conn

    # 关闭连接
    def conn_close(self,conn=None):
        conn.close()

    # 关闭连接，并释放
    def conn_trans(func):
        def connection(self,*args,**kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self,*args,**kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs
        return connection

    #执行指令
    @conn_trans
    def execute(self,command,method_flag=0,conn=None):
        cu = conn.cursor()

        if not method_flag:
            cu.execute(command)
        else:
            cu.execute(command[0],command[1])
        conn.commit()

    # 获取所有数据
    @conn_trans
    def fetchall(self,command="select name from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        cu.execute(command)
        lists=cu.fetchall()
        return lists


def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=[u'小区名称',u'大区域',u'小区域',u'小区户型',u'建造时间',u'价格',u'优势']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append(u'')
    t=tuple(t)
    command = (r"insert into xiaoqu values(NULL,?,?,?,?,?,?,?)",t)
    print('小区信息插入命令',command)
    return command


def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list = [u'小区名称',u'链接', u'签约日期', u'签约公司', u'户型', u'面积', u'房产总价', u'房产单价', u'挂牌价格',
                 u'房屋户型', u'所在楼层', u'建筑面积', u'户型结构', u'套内面积', u'建筑类型', u'房屋朝向', u'建成年代', u'装修情况',
                 u'建筑结构', u'供暖方式', u'梯户比例', u'产权年限', u'配备电梯', '链家编号', u'交易权属', u'挂牌时间', u'房屋用途', u'房屋年限', u'房权所属']
    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    print(len(t))
    command = (r"insert into chengjiao values(NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    print('成交记录插入命令',command)
    return command


def xiaoqu_spider(db_xq,url_page=u"http://bj.lianjia.com/xiaoqu/pg1rs%E6%98%8C%E5%B9%B3/"):
    """
    爬取页面链接中的小区信息
    """
    print('++++++++++++++++++++++++++++++++++',url_page)
    req = requests.get(url_page,headers=hds[random.randint(0,len(hds)-1)])
    plain_text=req.content.decode()
    soup = BeautifulSoup(plain_text,'html.parser')
    # print('小区信息',soup)
    xiaoqu_list=soup.findAll('li',{'class':'clear xiaoquListItem'})

    # print('小区信息',xiaoqu_list)

    for xq in xiaoqu_list:
        # print('----------------------------------',xq)

        # 小区名字
        xq_title = xq.find_all('div',{'class':'title'})[0].a.get_text()
        print('名字',xq_title)

        #小区详情
        xq_info_p = xq.find_all('div',{'class':'houseInfo'})[0]
        xq_info = ''
        for xq_infoDetail in xq_info_p.find_all('a'):

            # print('详细信息',xq_infoDetail.get_text())
            if xq_info:
                xq_info = xq_info + '-' + xq_infoDetail.get_text()
            else:
                xq_info = xq_infoDetail.get_text()

        # print('详情',xq_info)
        print('>>>>>>>>>>><<<<<<<<<<')

        #小区位置
        xq_position_p = xq.find_all('div',{'class':'positionInfo'})[0]
        xq_position_B = xq_position_p.find('a',{'class':'district'}).get('title')
        xq_position_S = xq_position_p.find('a',{'class':'bizcircle'}).get('title')


        xq_positionInfo = xq_position_p.text.replace(' ','').replace('\xa0','').replace('\n','')
        xq_positionInfoArr = xq_positionInfo.split('/')
        arrCount = len(xq_positionInfoArr)
        # print(xq_positionInfoArr)

        xq_type = '' #户型
        for i in range(1,arrCount-1):
            if xq_type:
                xq_type = xq_type + ',' + xq_positionInfoArr[i]
            else:
                xq_type = xq_positionInfoArr[i]

        # 优势
        xq_taglist = '未知'
        xq_taglistArr = xq.find_all('div',{'class': 'tagList'})
        xq_taglistFirst = xq_taglistArr[0].text.replace(' ', '').replace('\xa0', '').replace('\n', '')
        if xq_taglistFirst != '':
            xq_taglist = xq_taglistFirst

        xq_price_p = xq.find('div',{'class':'totalPrice'})

        xq_price = ''
        for price in xq_price_p.text:
            xq_price = xq_price + price

        # print('price',xq_price)
        # print(xq_type)
        # print('大',xq_position_B)
        # print('小',xq_position_S)


        info_dict={}
        info_dict.update({u'小区名称': space_string(xq_title)})
        info_dict.update({u'大区域': space_string(xq_position_B)})
        info_dict.update({u'小区域': space_string(xq_position_S)})
        info_dict.update({u'小区户型': space_string(xq_type)})
        info_dict.update({u'建造时间': space_string(xq_positionInfoArr[arrCount - 1])})
        info_dict.update({u'价格': space_string(xq_price)})
        info_dict.update({u'优势': space_string(xq_taglist)})
        command = gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command, 1)


def space_string(string):

    if string.isspace() == '':
        return '未知'
    else:
        return string


def do_xiaoqu_spider(db_xq,region = u"昌平"):
    """
    爬取大区域中的所有小区信息
    """
    url = u"http://bj.lianjia.com/xiaoqu/rs"+region+"/"
    print('小区信息获取')
    print(url)

    req = requests.get(url, headers=hds[random.randint(0,len(hds)-1)])
    plain_text=req.content.decode()
    soup = BeautifulSoup(plain_text, 'html.parser')
    d= soup.find('div',{'class': 'page-box house-lst-page-box'}).get('page-data')
    d = eval(d)
    totalPage = d['totalPage']
    total_pages = d['totalPage']

    threads=[]
    for i in range(total_pages):
        url_page = u"http://bj.lianjia.com/xiaoqu/pg%drs%s/" % (i+1, region)
        t = threading.Thread(target=xiaoqu_spider, args=(db_xq,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print (u"爬下了 %s 区全部的小区信息" % region)

#爬取单个页面链接中的成交记录
def chengjiao_spider(db_cj, url_page = u"http://bj.lianjia.com/chengjiao/pg1rs%E5%86%A0%E5%BA%AD%E5%9B%AD"):
    """
    爬取页面链接中的成交记录
    """
    # print(url_page)
    req = requests.get(url_page, headers=hds[random.randint(0,len(hds)-1)])
    plain_text = req.content.decode()
    soup = BeautifulSoup(plain_text, 'html.parser')
    cj_list = soup.findAll('ul',{'class': 'listContent'})[0]

    for cj in cj_list.findAll('li'):
        # print(cj)
        print('********')
        info_dict = {}
        href = cj.find('a')
        if not href:
            continue

        year = cj.find('div',{'class': 'dealDate'})
        print(year.text)
        if year:
            if year.text != '近30天内成交':
                yearcount = year.text.split('.')[0]
                if eval(yearcount) < 2016:
                    print('小于2016年')
                    return
        info_dict.update({u'链接':href.attrs['href']})

        # content = cj.find('div', {'class': 'title'}).text.split()
        # if content:
        #     info_dict.update({u'小区名称': content[0]})
        #     info_dict.update({u'户型': content[1]})
        #     info_dict.update({u'面积': content[2]})

        print(href.attrs['href'])

        req = requests.get(href.attrs['href'], headers=hds[random.randint(0, len(hds) - 1)])
        plain_text = req.content.decode()
        soup = BeautifulSoup(plain_text, 'html.parser')

        house_title = soup.find('div',{'class': 'house-title'})

        #签约日期
        house_date = house_title.span.text.split()
        if house_date:
            info_dict.update({u'签约日期': house_date[0]})

            if len(house_date) >= 2:
                info_dict.update({u'签约公司': house_date[1]})
            else:
                info_dict.update({u'签约公司': u'未知'})
        else:
            info_dict.update({u'签约日期': u'无'})
            info_dict.update({u'签约公司': u'未知'})

        #房产信息
        content = house_title.find('h1', {'class': 'index_h1'}).text.split()
        if content:
            info_dict.update({u'小区名称': content[0]})
            info_dict.update({u'户型': content[1]})
            info_dict.update({u'面积': content[2]})

        #房产infor
        house_infor = soup.find('div',{'class': 'info fr'})
        house_price = house_infor.find('div',{'class': 'price'})
        if house_price.find('span'):
            # print(house_price.span.text)
            info_dict.update({u'房产总价': house_price.span.i.text})
            info_dict.update({u'房产单价': house_price.b.text})
        else:
            # print(house_price.text)
            info_dict.update({u'房产总价': house_price.text})
            info_dict.update({u'房产单价': house_price.text})

        house_msg = house_infor.find('div', {'class': 'msg'})
        if house_msg:

            house_msg_span = house_msg.find_all('span')
            if house_msg_span:
                info_dict.update({u'挂牌价格': u'挂牌：' + house_msg_span[0].label.text + '万'})
            else:
                info_dict.update({u'挂牌价格': '无'})
        else:
            info_dict.update({u'挂牌价格': '无'})

        house_introcontent = soup.find('div',{'class': 'introContent'})
        if house_introcontent :
            basecontent = house_introcontent.find('div', {'class': 'base'}).find('div', {'class': 'content'}).find_all('li')#房子基本信息
            if basecontent :
                for sub_content in basecontent:
                    info_dict.update({sub_content.span.text.strip(): sub_content.text.strip().replace(sub_content.span.text.strip(),'')})

            transactioncontent = house_introcontent.find('div', {'class': 'transaction'}).find('div', {'class': 'content'}).find_all('li')#房子交易信息
            if transactioncontent :
                for sub_t_content in transactioncontent:
                    info_dict.update({sub_t_content.span.text.strip(): sub_t_content.text.strip().replace(sub_t_content.span.text.strip(),'')})

        # print(info_dict)

        command = gen_chengjiao_insert_command(info_dict)
        # ****************************
        db_cj.execute(command,1)

#爬取小区成交记录 循环抓取
def xiaoqu_chengjiao_spider(db_cj, xq_name = u"冠庭园"):
    """
    爬取小区成交记录
    """
    if xq_name == '小区名称':
        return

    url=u"http://bj.lianjia.com/chengjiao/rs"+xq_name+"/"
    print('爬取小区成交记录',url)

    req = requests.get(url,headers=hds[random.randint(0,len(hds)-1)])
    plain_text=req.content.decode()
    soup = BeautifulSoup(plain_text,'html.parser')

    content = soup.find('div',{'class':'page-box house-lst-page-box'})
    total_pages = 0
    if content:
        d = content.get('page-data')
        # d = exec(d)
        mynewdic = eval(d)
        print(type(mynewdic))
        total_pages = mynewdic['totalPage']

    threads = []
    for i in range(total_pages):
        url_page = u"http://bj.lianjia.com/chengjiao/pg%drs%s/" % (i+1, xq_name)
        t = threading.Thread(target=chengjiao_spider,args=(db_cj, url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

#批量爬取小区成交记录
def do_xiaoqu_chengjiao_spider(db_xq, db_cj):
    """
    批量爬取小区成交记录
    """
    count = 0
    xq_list = db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj, xq[0])
        count+= 1
        print ('have spidered %d xiaoqu' % count)
    print ('done')


def exception_write(fun_name,url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open(logPath,'a')
    line = "%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    """
    从日志中读取异常信息
    """
    lock.acquire()
    f = open(logPath,'r')
    lines = f.readlines()
    f.close()
    f = open(logPath,'w')
    f.truncate()
    f.close()
    lock.release()
    return lines


def exception_spider(db_cj):
    """
    重新爬取爬取异常的链接
    """
    count=0
    excep_list = exception_read()
    while excep_list:
        for excep in excep_list:
            excep = excep.strip()
            if excep == "":
                continue
            excep_name,url = excep.split(" ",1)
            if excep_name == "chengjiao_spider":
                chengjiao_spider(db_cj,url)
                count += 1
            elif excep_name == "xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj,url)
                count += 1
            else:
                print ("wrong format")
            print ("have spidered %d exception url" % count)
        excep_list = exception_read()
    print ('all done ^_^')



if __name__ == "__main__":

    command = "create table if not exists xiaoqu (id integer PRIMARY KEY AUTOINCREMENT,name TEXT , regionb TEXT, regions TEXT, style TEXT, year TEXT,price TEXT,advantage TEXT)"
    db_xq = SQLiteWraper('lianjia-xq.db', command)
    info_list = [u'小区名称',u'大区域',u'小区域',u'小区户型',u'建造时间',u'价格',u'优势']
    info_list = tuple(info_list)
    command = (r"insert into xiaoqu values(NULL,?,?,?,?,?,?,?)", info_list)
    db_xq.execute(command, 1)

    command = "create table if not exists chengjiao (id integer PRIMARY KEY AUTOINCREMENT, name TEXT,a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT, g TEXT, h TEXT, i TEXT,j TEXT, k TEXT, l TEXT, m TEXT, n TEXT, o TEXT, p TEXT,q TEXT, r TEXT, s TEXT, t TEXT, u TEXT, v TEXT, w TEXT,x TEXT, y TEXT, z TEXT, a1 TEXT, a2 TEXT)"
    db_cj = SQLiteWraper('lianjia-cj.db', command)

    info_list=[u'小区名称',u'链接', u'签约日期', u'签约公司', u'户型', u'面积', u'房产总价', u'房产单价', u'挂牌价格', u'房屋户型', u'所在楼层', u'建筑面积', u'户型结构', u'套内面积', u'建筑类型', u'房屋朝向', u'建成年代', u'装修情况', u'建筑结构', u'供暖方式', u'梯户比例', u'产权年限', u'配备电梯', '链家编号', u'交易权属', u'挂牌时间', u'房屋用途', u'房屋年限', u'房权所属']
    info_list = tuple(info_list)
    command = (r"insert into chengjiao values(NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", info_list)
    db_cj.execute(command, 1)

    #爬下所有的小区信息
    for region in regions:
        do_xiaoqu_spider(db_xq,region)

    #爬下所有小区里的成交信息
    do_xiaoqu_chengjiao_spider(db_xq, db_cj)

    #重新爬取爬取异常的链接
    exception_spider(db_cj)
    # xiaoqu_chengjiao_spider('冠庭园')
