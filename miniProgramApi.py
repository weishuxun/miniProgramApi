# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 22:06:25 2019

@author: with me
"""

import requests, json
import pandas as pd

class miniProgramApi():
    def __init__(self, env_name, appid, secret):
        self.env_name = env_name
        self.getAcsToken_url = "https://api.weixin.qq.com/cgi-bin/token"
        self.getAcsPar = {
                "grant_type" : "client_credential",
                "appid":appid, #自己的appid
                "secret":secret #自己的secret
                }
        self.addDbCollection_url = "https://api.weixin.qq.com/tcb/databasecollectionadd?access_token={}"
        self.delDbCollection_url = "https://api.weixin.qq.com/tcb/databasecollectiondelete?access_token={}"
        self.uploadFile_url = "https://api.weixin.qq.com/tcb/uploadfile?access_token={}"
        self.importItems_url = "https://api.weixin.qq.com/tcb/databasemigrateimport?access_token={}"
        self.headers = {
                "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
                }
        
    def getAccessToken(self):
        res = requests.get(self.getAcsToken_url, headers = self.headers, params=self.getAcsPar)
        acsToken = json.loads(res.content.decode())['access_token']
        return acsToken
    
    def addDbCollection(self, name):
        acsToken = self.getAccessToken()
        addDataPar = {
                "env" : self.env_name,
                "collection_name" : name
                }
        res = requests.post(self.addDbCollection_url.format(acsToken), data=json.dumps(addDataPar), headers = self.headers)
        status = json.loads(res.content.decode())["errcode"]
        if status == 0:
            print("数据表{}添加成功！".format(name))
        else:
            print("数据表{}添加失败！".format(name))
    
    def delDbCollection(self, name):
        acsToken = self.getAccessToken()
        addDataPar = {
                "env" : self.env_name,
                "collection_name" : name
                }
        res = requests.post(self.delDbCollection_url.format(acsToken), data=json.dumps(addDataPar), headers = self.headers)
        status = json.loads(res.content.decode())["errcode"]
        if status == 0:
            print("数据表{}删除成功！".format(name))
        else:
            print("数据表{}删除失败！".format(name))
        
    def importItems(self, collectionName, file):
        '''注意传入file文件必须是dataframe按index转换成dict的文件格式'''
        #1.新建上传位置
        up_headers = self.headers.update({"content-type":"multipart/form-data; boundary=c889e2cd4e2470630d99dc2fe26a443d"})
        acsToken = self.getAccessToken()
        upDataPar = {
                "env" : self.env_name,
                "path" : "Data.json"
                }
        res = requests.post(self.uploadFile_url.format(acsToken), data=json.dumps(upDataPar), headers = self.headers).content.decode()
        res = json.loads(res)
        #2.新建上传临时文件
        url = res["url"]
        content = ""
        for i in range(len(file)):
            content = content + str(file[i]) +"\n" 
        updata = {
                "key":upDataPar["path"],
                "Signature":res["authorization"],
                "x-cos-security-token":res["token"],
                "x-cos-meta-fileid":res["cos_file_id"]
                }
        file = {"file":content}
        status = requests.post(url, data = updata, files = file, headers = up_headers)
        print("上传响应码为：{}".format(status.status_code))
        #3.添加文件至数据库
        addDataPar = {
                "env": self.env_name,
                "collection_name": collectionName,
                "file_path":"Data.json",
                "file_type":1,
                "stop_on_error": False,
                "conflict_mode": 1
                }
        res = requests.post(self.importItems_url.format(acsToken), data=json.dumps(addDataPar), headers = self.headers)
        status = json.loads(res.content.decode())["errcode"]
        if status == 0:
            print("数据添加成功！")
        else:
            print("数据添加失败！")
    
    def run(self):
        file = [{"name":"小明","height":190.34355666,"weight":23},
                {"name":"小红","height":34.23567,"weight":12},
                {"name":"小兰","height":79.4802940,"weight":5}]
        df = pd.DataFrame(file)
        df = df.to_dict(orient = "index")
        self.addDbCollection("test")
        self.importItems("test", df)
        


if __name__ == "__main__":
    mini_program = miniProgramApi(添加自己的参数)
    mini_program.run()
    