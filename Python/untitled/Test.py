# -*- coding: utf-8 -*-
import requests
import os
import time
from bs4 import BeautifulSoup


def startConver():

    if login():
        print('登陆成功')
        path = '/Users/Qiushan/Desktop/PDF图书待转换'
        circleAllImages(path)

#循环上传
def circleAllImages(dirPath):
    # uploadImage(dirPath)

    for fileName in os.listdir(dirPath):
        if '.jpg' in fileName:
            fullPath = dirPath+'/'+fileName
            uploadImage(fullPath)

#cookie
def cookieStr():
    return 'PHPSESSID=rc4fcweds5keolff0u7adscgkv0; Hm_lvt_c28bc0ecf7d4c8asd9eafd4sefd0653e0=1517809710'
# 'Cookie':'PHPSESSID=rc4fcto95keolff0u7uicggkv0; Hm_lvt_c28bc0ecf7d4c8b69eafd431f90653e0=1517809710'

#userAgent
def user_agent():
    return 'Mozilla/4.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/531.36 (KHTML, like Gecko) Chrome/63.0.3139.132 Safari/517.36'
#'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'

# header
def urlHeader():
    return {
        'User-Agent': user_agent(),
        'Accept': 'image/webp,image/apng,*/*;q=0.8',
        'Referer': 'http://ocr.wdku.net/public/uploader.css?v=20171024',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie':cookieStr()
    }


#login header
def loginHeader():
    return {
        'User-Agent': user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'http://www.wdku.net/user/login',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Origin': 'http://www.wdku.net',
        'Cookie': cookieStr()
    }

# 登陆
def login():
    url = 'http://www.wdku.net/user/login'
    data = {
    'email':'456789000@qq.com',
    'pass':789456
    }
    respose = requests.post(url,headers=loginHeader(),data=data)
    # print(respose)
    # print(respose.content.decode())
    soup = BeautifulSoup(respose.content.decode(),'html.parser')
    # print(soup)
    for p in soup.find_all('p'):
        if p.string == '登陆成功':
            return 1


# 上传
def uploadImage(fullpath):
    url = "http://ocr.wdku.net/Upload"
    file_name = os.path.split(fullpath)[-1]
    sStr1 = 'page' + file_name[13:]
    print(sStr1)
    # 要上传的文件
    file = {'file': (sStr1, open(fullpath, 'rb'))
            }  # 显式的设置文件名

    print(file)

    response = requests.post(url, files=file, headers=urlHeader())
    responseJson = response.json()
    print(responseJson)
    print('上传JSON = %s' % response.content)
    idStr = responseJson['data']['id']
    titleStr = responseJson['data']['title']
    print(idStr)
    converJpg(idStr,titleStr)


# 开始转换
def converJpg(idStr,titleStr):
    url = 'http://ocr.wdku.net/submitOcr?type=1'
    localTime = int(time.time())
    data = {
        'obj_type':'pdf',
        'ids':idStr,
        'ts':localTime,
        'pass':'',
        'lang':'1,2'
    }

    response = requests.post(url,headers=urlHeader(),data=data)
    print('开始转换JSON = %s' % response.content)
    responseJson = response.json()
    print(responseJson)
    if responseJson['errno'] == 0:
        idStr = responseJson['id']
        timeStr = responseJson['time']
        checkState(idStr,timeStr,titleStr)
    # "errno": 0,
    # "desc": "Success",
    # "id": "ppZYOv",
    # "time": 1517986914

#检查状态
def checkState(idStr,timeStr,titleStr):
    url = 'http://ocr.wdku.net/waitResult2?id='+idStr+'&_'+timeStr
    response = requests.get(url,headers=urlHeader())
    print('检查状态JSON = %s' % response.content)
    responseJson = response.json()
    if responseJson['status'] == 1:
        downloadPdf(idStr,timeStr,titleStr)
        return
    else:
        checkState(idStr,timeStr+1,titleStr)
    print(response.status_code)

# 开始下载
def downloadPdf(idStr,timeStr,titleStr):
    url = 'http://ocr.wdku.net/downResult?id=' + idStr + '&t' + timeStr
    response = requests.get(url, headers=urlHeader())

    filepath = '/Users/Qiushan/Desktop/PDF图书待转换完成/'+titleStr+'.jpg'
    with open(filepath, "wb") as code:
        code.write(response.content)


if __name__ == '__main__':
    print('开始了')
    startConver()