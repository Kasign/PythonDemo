# -*- coding: utf-8 -*-
import os

path = '/Users/Qiushan/Desktop/PDF图书待转换'
# f = open('/Users/Qiushan/Desktop/接口.rtf')
# data = f.read()
# f.close()
# print(data)
for filename in os.listdir(r'/Users/Qiushan/Desktop/PDF图书待转换'):
    fullPath = path + filename
    if ('.DS_Store' in fullPath):
        break

    imageFile = open(fullPath)
    content = imageFile.read()
    imageFile.close()

    print(fullPath)
    print(content)

