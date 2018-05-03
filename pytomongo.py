# -*- coding: utf-8 -*-
# author:maguichang time:2018/4/3
import pymongo
import matplotlib.pylab as plt
import pandas as pd
conn = pymongo.MongoClient("10.0.7.37", 27017)
db = conn.Base #连接Bat数据库，没有则自动创建
my_set = db.Test_Fan3
li = []
# 8640
for i in my_set.find({}).sort("_id",-1).limit(180).skip(180):

    li.append((i["_id"],i["wind_speed_10"],i["active_power_10"]))
# 此处不要去重，去重之后时间不连续
# li_set=list(set(li))
# print(len(li_set))
date_time=[]
wind_data=[]
power_data=[]
for i in li:
    date_time.append(i[0])
    wind_data.append(i[1])
    power_data.append(i[2])
    #print(i["PV_ActivePower"])
#plt.plot(x_data,y_data)

# 将列表转化为数据框
data = {"date_time":date_time,"wind_data":wind_data}

plt.plot(date_time,wind_data)
plt.show()
plt.scatter(wind_data,power_data)
plt.show()
# x_data,y_data为去重之后的实际风速与功率数据
# 需要基于以上数据，构建训练与测试模型
# 随机取60%训练，40%做测试，计算误差


