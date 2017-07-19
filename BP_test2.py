# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 10:33:40 2017

@author: dell
"""

##BP测试对应BP2程序,经过验证已OK！数据处理已完成归一化和反归一化。
from BP2 import BPNeuralNetwork
import numpy as np
nn = BPNeuralNetwork()

##风功率曲线对应的风速数据
x=[0,0.5,1,1.5,2,2.5,3,3.5,4,4.5,5,5.5,6,6.5,7,7.5,8,8.5,9,9.5,10,10.5,11,11.5,12,12.5,13,13.5,14,14.5,15,15.5,16,16.5,17,17.5,18,18.5,19,20,20.5,21,21.5,22,22.5,23,23.5,24,24.5,25]
x2=[]
for i in xrange(len(x)):
    x2.append((x[i]-min(x))/float(max(x)-min(x)))

##将一个列表转化为列表嵌套的的形式
##输入训练数据，读入mongo的风速数据
#原始
x1=[[i] for i in x2]
##风功率曲线对应的风功率数据
y=[0,0,0,0,0,0,3,7,13,21,31,42,56,73,92,114,140,168,185,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200,200]
#原始
y2=[]
for i in xrange(len(y)):
    y2.append((y[i]-min(y))/float((max(y)-min(y))))

y1=[[i] for i in y2]
nn.setup(1, 3, 1)
nn.train(x1,y1,10000,0.05,0.1)
    
t2=[]
##调用天气接口获得的预测的风速数据，先存到mongo，再从mongo中调用出来存入列表作为输入。
t=[5,2,3,4,6,10,20,22,23,25]
for i in xrange(len(t)):
    t2.append((t[i]-0)/25.0)
t1=[[i] for i in t2]
num=[]
for i in t1:
    print (i,nn.predict(i))
    num.append(nn.predict(i))
    
print num
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
    if output[i][0]<3:
        output[i][1]=0
    elif output[i][0]>10:
        output[i][1]=200
        
        
##最后结果输出为output。
##如果需要将output的数据写到mongo。
##注：未进行归一化的输入数据由于值太大，导致输出数据都是1.
##风速每隔一个小时更新一次。

        
            
    
            
        