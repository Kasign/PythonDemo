import requests

url = 'http://www.test.com/doFile.php'
# url = 'http://www.test.com/doPost.php'
# files = {'file': open('D:/tmp/1.jpg', 'rb')}

# 要上传的文件
files = {'file': ('1.jpg', open('/Users/Qiushan/Desktop/PDF图书待转换/图灵程序设计丛书 算法 第4版24.jpg', 'rb'))
         }  # 显式的设置文件名

# post携带的数据
data = {'a': '杨', 'b': 'hello'}

r = requests.post(url, files=files, data=data)
print(r.text)