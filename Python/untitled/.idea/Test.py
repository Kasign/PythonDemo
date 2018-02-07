# -*- coding: utf-8 -*-
import requests
import os
import time
# import json

def startConver():
    path='/Users/Qiushan/Desktop/PDF图书待转换'
    circleAllImages(path)

#循环上传
def circleAllImages(dirPath):
    # uploadImage(dirPath)
    for fileName in os.listdir(dirPath):
        if '.jpg' in fileName:
            fullPath = dirPath+'/'+fileName
            uploadImage(fullPath)

def urlHeader():
    return {
        'Host': 'ocr.wdku.net',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Referer': 'http://ocr.wdku.net/public/uploader.css?v=20171024',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie':'PHPSESSID=rc4fcto95kasqwwqibz0; Hm_lvt_c28bc0ecf7d4c8b69eafsfesaf653e0=1510809710'
    }
# 'Cookie':'PHPSESSID=rc4fcto95keolff0u7uicggkv0; Hm_lvt_c28bc0ecf7d4c8b69eafd431f90653e0=1517809710'

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
    id = responseJson['data']['id']
    print(id)
    converJpg(id)


# 开始转换
def converJpg(idStr):
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
    # print(responseJson)
    if responseJson['errno'] == 0:
        idStr = responseJson['id']
        timeStr = responseJson['time']
        checkState(id,timeStr)
    # "errno": 0,
    # "desc": "Success",
    # "id": "ppZYOv",
    # "time": 1517986914

#检查状态
def checkState(idStr,timeStr):
    url = 'http://ocr.wdku.net/waitResult2?id='+idStr+'&_'+timeStr
    response = requests.get(url,headers=urlHeader())
    print('检查状态JSON = %s' % response.content)
    if response.status_code == 1:
        downloadPdf(idStr,timeStr + 1)
        return
    else:
        checkState(idStr,timeStr+1)
    print(response.status_code)

# 开始下载
def downloadPdf(idStr,time):
    url = 'http://ocr.wdku.net/downResult?id=' + idStr + '&t' + time
    response = requests.get(url, headers=urlHeader())

    filepath = '/Users/Qiushan/Desktop/PDF图书待转换完成/'+time+'.jpg'
    with open(filepath, "wb") as code:
        code.write(response.content)


if __name__ == '__main__':
    print('开始了')
    startConver()