# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 11:47:12 2018

@author: Administrator
"""

# python批量处理txt

# coding=utf-8
import os

'''
if __name__ == '__main__':
    
    mypath='D:\\upload\\logData\\3\\20180328\\084\\99#'#定义文件存储路径的文件夹
    myfile=[]#存储文件夹下所有文件的文件名
    for f in os.listdir(mypath):
        print f
        myfile.append(f)
    
    for i in myfile:#批量对文件夹下每一个文件操作
        filepath=mypath+'\\'+i
        with open(filepath,'r') as foo:
            for line in foo.readlines():
                if '03_05^102' in line:
                    print line+i
'''                    
#更新版

if __name__ == '__main__':
    
    #mypath='D:\\upload\\logData\\3\\20180328\\084'#定义文件存储路径的文件夹
    mypath='C:\\Users\\dell\\Desktop\\logdata\\log'#定义文件存储路径的文件夹
    mydir=[]
    for d in os.listdir(mypath):
        #print d
        mydir.append(d)
        
    for d in mydir:
        myfilepath='C:\\Users\\dell\\Desktop\\logdata\\log'+'\\'+d
        myfile=[]
        for f in os.listdir(myfilepath):
            filepath=myfilepath+'\\'+f
            with open(filepath,'r') as foo:
                data = foo.readlines()
                len_data=[]
                for l in data[12:]:
                    len_data.append(l.split('\t'))    
                for i in len_data:
                    #print len(i)
                    if len(i)>12:
                        print len(i)
                        #print i
                        #print filepath
   
            
