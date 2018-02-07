# -*- coding: utf-8 -*-
# Author:ma
# @Time :2018/2/7 9:48
import os,xlrd
def open_excel(file_path):
    data = xlrd.open_workbook(file_path)
    table = data.sheets()[0]
    nrows = table.nrows
    print(nrows)
    return table

if __name__ == "__main__":
    file_name = []
    for f in os.listdir('C:/Users/dell/Desktop/典型风场/dxfc'):
        print("file:", f)
        file_name.append(f)
    for i in file_name:
        file_path='C:/Users/dell/Desktop/典型风场/dxfc/'+i
        open_excel(file_path)