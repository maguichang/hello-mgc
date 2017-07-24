# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 13:33:32 2017

@author: dell
"""

#Ppython 调用彩云天气预报风速数据并将数据导入mongo数据库
#未完成，定时执行该程序，更新风速，风功率
import requests
import json
##更改经纬度以更改城市或位置
url=r'http://api.caiyunapp.com/v2/Rrk8X6aIhsCi4s5K/117.183,39.15/forecast.json'

jsonStr = requests.get(url).text



data = eval(json.dumps(jsonStr,ensure_ascii=False))
exec("Data="+data)
##本句出现错误
info = Data["result"]
weather = info["hourly"]
predict_wind=weather["wind"]
###如果需要导入数据到mysql数据库，可以新建list，把数据追加到list再导入mysql
'''
p_time=[]
p_direction=[]
p_speed=[]
for t in predict_wind:
    p_time.append(t["datetime"])
    p_direction.append(t["direction"])
    p_speed.append(t["speed"])
'''   
import pymongo
conn=pymongo.MongoClient(host="10.0.1.163")
db=conn.mydb

##连接mongo并插入数据，直接将字典组成的list插入到mongo
#db.createCollection("myPredict")
MP=db.WindPredict2
record_1=predict_wind

for record in record_1:
    MP.save(record)
for x in MP.find():
    print x
##更新_id为时间


##预测算法，根据风速预测风功率
###note：风速与主轴转速和电机转速，由于相关系数较大，可以认为它们之间存在一个线性关系，
###通过简单线性拟合，根据天气预报的风速推断出主轴转速和电机转速。然后以这三个变量为
###输入变量，与风功率进行多元线性回归计算，预测风功率数据。

