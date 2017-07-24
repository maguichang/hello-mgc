# -*- coding: utf-8 -*-
"""
Created on Mon Jul 24 15:58:16 2017

@author: dell
"""

###最终版风功率预测
f = open(r'C:/Users/dell/Desktop/data_test.txt', 'r')
ch=f.readlines()
##读取的每一行的末尾由于有一个回车符，出现"\n".需要将给字符去掉。否则会出错。此外，最后一行没有关系
###如何以字典形式返回数值
ip=ch[1][:-1]
url=ch[3][:-1]
mdb=ch[5][:-1]
mc=ch[7][:-1]
path=ch[9][:]
##以下这种通过mongo用户的写法，数据库名称，collection。可以以读取txt文档的方式写入。,以验证OK！

from pymongo import MongoClient

client = MongoClient(ip, 27017)
db_name = mdb
db = client[db_name]
MP = db[mc]
p_data=[]
datetime=[]
for x in MP.find():
    p_data.append(x)
wind_predict=[]
for i in xrange(len(p_data)):
    wind_predict.append(p_data[i]['speed_predict']*1000/3600.0)
    datetime.append(p_data[i]['_id'])
##完成预测风速的读取
##载入预测模型
from sklearn.externals import joblib
loaded_model = joblib.load(path)
t2=[]
##调用天气接口获得的预测的风速数据，先存到mongo，再从mongo中调用出来存入列表作为输入。
t=wind_predict

for i in xrange(len(t)):
    t2.append((t[i]-0)/10.0)
t1=[[i] for i in t2]
num=[]
for i in t1:
#    print (i,loaded_model.predict(i))
    num.append(loaded_model.predict(i))
    
#print num
num2=[]
for i in xrange(len(num)):
    num2.append(num[i][0]*200)
##对于输出结果，做一个替换操作，将风速小于3所对应的风功率数据替换为0。将风速大于10所对应的风功率数据替换为200
##元组内容不可变，不能这样转化为元组
#output=[t,num2]
###对天气预报的接口调用的风速数据，根据风功率曲线做一个判断和替换。对于风速
#小于2m/s的预测风功率置为0，对于风速大于10m/s的预测风功率置为200
output=[[t[i],num2[i]] for i in xrange(len(t))]
for i in xrange(len(output)):
    if output[i][0]<2:
        output[i][1]=0
    elif output[i][0]>10:
        output[i][1]=200
               
##最后结果输出为output。
##如果需要将output的数据写到mongo。
##注：未进行归一化的输入数据由于值太大，导致输出数据都是1.
##风速每隔一个小时更新一次。
###将output数据写入mongo
MP=db.wsp_Predict
########先将list转化为dict的列表再写入mongo,一个一个字段分别导入会出现数据错位的现象
wp=[]
for i in xrange(len(wind_predict)):
    wp.append({'speed_predict':wind_predict[i]})
#for x in wp:
#    MP.save(x)
tp=[]
for j in xrange(len(datetime)):
    tp.append({'datetime':datetime[j]})   
#for y in tp:
#    MP.save(y)
pp=[]
for k in xrange(len(output)):
    pp.append({'power_predict':output[k][1]})
#for z in pp:
#    MP.save(z)
##整合三个字段，时间，风速，风功率
p=[[wp[i]['speed_predict'],tp[i]['datetime'],pp[i]['power_predict']] for i in xrange(len(wind_predict))]
p_data=[]
##_id就是时间
for i in xrange(len(p)):
    p_data.append({'speed_predict':p[i][0],'_id':p[i][1],'power_predict':p[i][2]})
for i in p_data:
    MP.save(i)

#
   
    