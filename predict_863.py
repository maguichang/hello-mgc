# -*- coding: utf-8 -*-
"""
Created on Mon Jul 24 09:44:33 2017

@author: dell
"""
##归一化除以25，还是有一点问题，待解决(除以10效果好一点)
###调用wind_power_train_model,预测风功率
##连接mongodb读取预测的风速数据
import pymongo
conn=pymongo.MongoClient(host="10.0.1.163")
db=conn.mydb
MP=db.WindPredict
p_data=[]
datetime=[]
for x in MP.find():
    p_data.append(x)
wind_predict=[]
for i in xrange(len(p_data)):
    wind_predict.append(p_data[i]['speed'])
    datetime.append(p_data[i]['datetime'])
##完成预测风速的读取
##载入预测模型
from sklearn.externals import joblib
loaded_model = joblib.load("C:/Users/dell/Desktop/train_model_863.m")
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
#小于3m/s的预测风功率置为0，对于风速大于10m/s的预测风功率置为200
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
for i in xrange(len(p)):
    p_data.append({'speed_predict':p[i][0],'datetime':p[i][1],'power_predict':p[i][2]})
for i in p_data:
    MP.save(i)

#
   
    