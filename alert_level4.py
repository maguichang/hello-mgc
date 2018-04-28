# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 08:36:49 2017

@author: dell
"""

####预警4，添加规则创建人

import datetime
###故障预警更新版
def run_Task1():
    import pymysql
    
    conn= pymysql.Connect(
            
            host='10.0.1.144',
            port = 3306,
            user='mgc',
            passwd='123456',
            db ='resmartest',
            charset='utf8'
            )
    cur = conn.cursor()
    print conn
    print cur
    sql1 = "select distinct(rules_id) from eg_bus_alert_rules"
    cur.execute(sql1)
        # 获取查询结果
    result_set1 = cur.fetchall()
        ########获取四层循环的数据   
    import pandas as pd
    import numpy as np
        #rules_id,windfarm_id,turbine_id,trigkey
    r_id=[[ij for ij in i] for i in result_set1]
    r_id_new=[r_id[i][0] for i in xrange(len(r_id))]
        
        
    x=[]
    y=[]
    z=[]
    #    w=[]
    for k1 in r_id_new:
        sql2= "select distinct(windfarm_id) from eg_bus_alert_rules where rules_id= "+str(k1)
        cur.execute(sql2)
        result_set2= cur.fetchall()
        #解决转化数据格式的问题，即可完成三层循环的书写。
        #f_id=[[ij for ij in i] for i in result_set2]
        f_id=[[ij for ij in i] for i in result_set2]
        f_id_new=[f_id[i][0] for i in xrange(len(f_id))]
        #x只是搜集规则下的风场数据，并没有什么作用
        x.append(f_id_new)
    
        for k2 in f_id_new:
            sql3="select distinct(turbine_id) from eg_bus_alert_rules where rules_id= "+str(k1)+" and windfarm_id= "+str(k2)
            cur.execute(sql3)
            result_set3=cur.fetchall()
            t_id=[[ij for ij in i] for i in result_set3]
            t_id_new=[t_id[i][0] for i in xrange(len(t_id))]
            y.append(t_id_new)
            for k3 in t_id_new:
                sql4="select distinct(left(trigkeys,11)) from eg_bus_alert_rules where rules_id = "+str(k1)+" and windfarm_id = "+str(k2)+" and turbine_id = "+str(k3)
                cur.execute(sql4)
                result_set4=cur.fetchall()
                tk=[[ij for ij in i] for i in result_set4]
                tk_new=[tk[i][0] for i in xrange(len(tk))]
                z.append(tk_new)
                for k4 in tk_new:
                    ##改变最内层循环，实现mysql的功能。拼接mysql的语句。python调用mysql的存储过程
                    ##取FaultDays
                  
                    sql5="select faultdays from eg_bus_alert_rules where rules_id= "+str(k1)+" and windfarm_id = "+str(k2)+" and turbine_id = "+str(k3)+" and left(trigkeys,11)= '"+str(k4)+"'"
                    cur.execute(sql5)
                    result_set5=cur.fetchall()
                    fd=[[ij for ij in i] for i in result_set5]
                    fd_new=[fd[i][0] for i in xrange(len(fd))]
                    sql6="DROP table IF EXISTS tp"
                    cur.execute(sql6)
                    '''
                    sql7="create table tp select * from (select create_user,faultDays,windfarm,sendtype,rules_id,windfarm_id,turbine,turbine_id,left(trigkeys,11) new_trigkeys,level1low,level1high,level2low,level2high,\
    level3low FROM eg_bus_alert_rules where rules_id= "+str(k1)+" and windfarm_id= "+str(k2)+" and turbine_id= "+str(k3)+" and tgk= '"+str(k4)+"'"")a LEFT JOIN \
    (select trigkey,count(trigkey) numtrigkey from eg_fut_farm"+str(k2)+" WHERE turbine = "+str(k3)+" and trigkey = '"+str(k4)+"'"" and TimeStampUTC > SUBDATE(NOW(),INTERVAL "+str(fd_new[0])+" DAY) and TimeStampUTC <now() and isrepeat!=1 and isreleated='N' GROUP BY trigkey)b on a.new_trigkeys=b.trigkey"
                    '''
                    sql7="create table tp select * from (select create_user,faultDays,windfarm,sendtype,rules_id,windfarm_id,turbine,turbine_id,left(trigkeys,11) new_trigkeys,level1low,level1high,level2low,level2high,\
    level3low FROM eg_bus_alert_rules where rules_id= "+str(k1)+" and windfarm_id= "+str(k2)+" and turbine_id= "+str(k3)+" and left(trigkeys,11)= '"+str(k4)+"'"")a LEFT JOIN \
    (select trigkey,count(trigkey) numtrigkey from eg_fut_farm"+str(k2)+" WHERE turbine = "+str(k3)+" and trigkey = '"+str(k4)+"'"" and TimeStampUTC > SUBDATE(NOW(),INTERVAL "+str(fd_new[0])+" DAY) and TimeStampUTC <now() GROUP BY trigkey)b on a.new_trigkeys=b.trigkey"
                    cur.execute(sql7)
                    sql8="ALTER TABLE tp add (alert_level_ma int,alert_level_ma_fj INT,alert_level_ma_fc INT,\
    numtrigkey_fj INT,numtrigkey_fc INT,alert_name_ma VARCHAR(255),alert_desc_ma VARCHAR(255),\
    alert_location_ma VARCHAR(255),alert_type_ma INT,alert_date_ma date,create_time_ma datetime,\
    fault_code_ma VARCHAR(255))"
                    cur.execute(sql8)
                    sql9="INSERT INTO result_all2(rules_id, windfarm_id,windfarm,turbine,turbine_id,trigkeys,\
    level1low,level1high,level2low,level2high,level3low,num1_trigkey,create_time_ma,sendtype,faultDays,create_user_id)\
    SELECT rules_id,windfarm_id,windfarm,turbine,turbine_id,new_trigkeys,level1low,level1high,level2low,\
    level2high,level3low,numtrigkey,create_time_ma,sendtype,faultDays,create_user from tp"
                    cur.execute(sql9)
                                      
                    #测试代码
                    #sql5="select count(*) from eg_fut_farm"+str(k2)+" where turbine = "+str(k3)+" and TrigKey= '"+str(k4)+"'"
                    #cur.execute(sql5)
                    #result_set5=cur.fetchall()
                    #c=[[ij for ij in i] for i in result_set5]
                    #c_new=[c[i][0] for i in xrange(len(c))]
                    #w.append(c_new)
                    ##判断c，是否需要执行下列语句
                    #sql6="create table tp select * from (select * from eg_bus_alert_rules where rules_id=k1 and windfarm_id)"
    ###调用mysql的存储过程，实现更新,生成中间结果表ma
    sql10="call update_alert_new()"
    cur.execute(sql10)
    ###如果你执行的语句会改变数据库内容，那么你必须commit一下才能生效。（重要）
    conn.autocommit(True)
    #########未参考history与lack表的情况
    
    #sql20="call insert_result2()"
    #cur.execute(sql20)
    #conn.autocommit(True)
    #sql31="drop table ma"
    #cur.execute(sql31)
    #conn.autocommit(True)
    #cur.close()
    #conn.close()
    #print("ok! start next task.")   
    ####参照history与lack表,是否将中间结果写入eg_but_result_all
    ##对于风场与风机、单条的情况各异，对风场的进行特殊处理
    #############针对风场的推送处理
    
    sql11_2="select sendtype from ma"
    cur.execute(sql11_2)
    result_set11_2=cur.fetchall()
    s_type=[[ij for ij in i] for i in result_set11_2]
    s_type_new2=[s_type[i][0] for i in xrange(len(s_type))]
    ###处理s_type_new,使其只有两种取值0与2为同一种
    for i in range(len(s_type_new2)):
        if s_type_new2[i]==2:
            s_type_new2[i]=0
    s_type_new=list(set(s_type_new2))
    
    for st in s_type_new:
        if st==0:
            
            ##pass部分执行以下源程序
            sql11="select turbine_id from ma where sendtype=0 or sendtype=2"
            cur.execute(sql11)
            result_set11=cur.fetchall()
            id_num=[[ij for ij in i] for i in result_set11]
            id_new=[id_num[i][0] for i in xrange(len(id_num))]
            
            ###防止重复turbine_id的干扰，需要id作为主键自增，不重复。更改建立result_all2的建表过程
            sql21="select id from ma where sendtype=0 or sendtype=2"
            cur.execute(sql21)
            result_set21=cur.fetchall()
            idid=[[ij for ij in i] for i in result_set21]
            idid_new=[idid[i][0] for i in xrange(len(idid))]
            ##组合id与风机号
            id_turbine=zip(idid_new,id_new)
            
            l=[]  
            for k11 in id_turbine:
                #调用存储过程，设置history与lack表
              
                sql12="call flag()"
                cur.execute(sql12)
                conn.autocommit(True)
                
                sql13="SELECT flag1 FROM eg_data_history_ma WHERE eg_data_history_ma.WTURBINE_ID = "+str(k11[1])
                cur.execute(sql13)
                result_set13=cur.fetchall()
                f=[[ij for ij in i] for i in result_set13]
                f_new=[f[i][0] for i in xrange(len(f))]#flag1的值
                l.append(f_new[0])
                
                sql14="SELECT right(upload_dates,8) t_end FROM eg_data_history_ma WHERE eg_data_history_ma.WTURBINE_ID = "+str(k11[1])
                cur.execute(sql14)
                result_set14=cur.fetchall()
                t_end=[[ij for ij in i] for i in result_set14]
                #t_end_new=[f[i][0] for i in xrange(len(t_end))]#history结束的日期的值
                t_end_new=t_end[0][0]
                
                sql15="select s_date from ma where id= "+str(k11[0])+" and turbine_id= "+str(k11[1])
                cur.execute(sql15)
                result_set15=cur.fetchall()
                sd=[[ij for ij in i] for i in result_set15]
                #此处需要对时间进行格式转换
                #sd_new=[f[i][0] for i in xrange(len(sd))]#now()-faultdays的值，查询起始时间值
                sd_new=datetime.datetime.strftime(sd[0][0],'%Y%m%d')
                if f_new[0]==1 and t_end_new > sd_new:
                    #sql22="call insert_result2()"#不能进行整体推送调用，需要加条件进行推送
                    
                    sql22="call in_result("+str(k11[0])+","+str(k11[1])+")"
                    cur.execute(sql22)
                    conn.autocommit(True)
                
                else:
                    #sql16="UPDATE eg_data_lack_ma SET flag2 = 1 WHERE CONCAT(WTURBINE_ID,LACK_DATES) IN (SELECT ct from lack) AND eg_data_lack_ma.LACK_DATES >"+str(sd_new[0])
                    #cur.execute(sql16)
                    #conn.autocommit(True)
                    sql16="call update_flag2()"
                    cur.execute(sql16)
                    conn.autocommit(True)
                    sql17="SELECT count(*) FROM eg_data_lack_ma WHERE WTURBINE_ID= "+str(k11[1])+" and flag2=1 AND LACK_DATES > "+str(sd_new)
                    cur.execute(sql17)
                    result_set17=cur.fetchall()
                    c=[[ij for ij in i] for i in result_set17]
                    c_new=c[0][0]
                    if c_new>0:
                        #sql18="call insert_result2()"
                        
                        sql18="call in_result("+str(k11[0])+","+str(k11[1])+")"
                        cur.execute(sql18)
                        conn.autocommit(True)
        else:
            sql11_3="select distinct(windfarm_id) from result_all where sendtype=1"
            cur.execute(sql11_3)
            result_set11_3=cur.fetchall()
            w_farm=[[ij for ij in i] for i in result_set11_3]
            w_farm_new=[w_farm[i][0] for i in xrange(len(w_farm))]
            l2=[]
            for wf in w_farm_new:
                sql11_4="select distinct(turbine_id) from result_all where sendtype=1 and windfarm_id= "+str(wf)
                cur.execute(sql11_4)
                result_set11_4=cur.fetchall()
                tb_id=[[ij for ij in i] for i in result_set11_4]
                tb_id_new=[tb_id[i][0] for i in xrange(len(tb_id))]
                
                for ti in tb_id_new:
                    
                    sql12_1="call flag()"
                    cur.execute(sql12_1)
                    conn.autocommit(True)
        
                    sql13_1="SELECT flag1 FROM eg_data_history_ma WHERE eg_data_history_ma.WTURBINE_ID = "+str(ti)
                    cur.execute(sql13_1)
                    result_set13_1=cur.fetchall()
                    f2=[[ij for ij in i] for i in result_set13_1]
                    f_new2=[f2[i][0] for i in xrange(len(f2))]#flag1的值
                    l2.append(f_new2[0])
        
                    sql14_1="SELECT right(upload_dates,8) t_end FROM eg_data_history_ma WHERE eg_data_history_ma.WTURBINE_ID = "+str(ti)
                    cur.execute(sql14_1)
                    result_set14_1=cur.fetchall()
                    t_end2=[[ij for ij in i] for i in result_set14_1]
        #t_end_new=[f[i][0] for i in xrange(len(t_end))]#history结束的日期的值
                    t_end_new2=t_end2[0][0]
                    ###################################s_date在result_all中的设定
        
                    sql15_1="select s_date from result_all where turbine_id= "+str(ti)
                    cur.execute(sql15_1)
                    result_set15_1=cur.fetchall()
                    sd2=[[ij for ij in i] for i in result_set15_1]
        #此处需要对时间进行格式转换
        #sd_new=[f[i][0] for i in xrange(len(sd))]#now()-faultdays的值，查询起始时间值
                    sd_new2=datetime.datetime.strftime(sd2[0][0],'%Y%m%d')
                    if f_new2[0]==1 and t_end_new2 > sd_new2:
                        
                        sql22_2="call in_result2("+str(wf)+")"
                        cur.execute(sql22_2)
                        conn.autocommit(True)
                        break
                    else:
                         sql16_1="call update_flag2()"
                         cur.execute(sql16_1)
                         conn.autocommit(True)
                         sql17_1="SELECT count(*) FROM eg_data_lack_ma WHERE WTURBINE_ID= "+str(ti)+" and flag2=1 AND LACK_DATES > "+str(sd_new2)
                         cur.execute(sql17_1)
                         result_set17_1=cur.fetchall()
                         c2=[[ij for ij in i] for i in result_set17_1]
                         c_new2=c2[0][0]
                         if c_new2>0:
                             #sql18="call insert_result2()"
                             sql22_3="call in_result2("+str(wf)+")"
                             cur.execute(sql22_3)
                             conn.autocommit(True)
                             break
                    
    
    #sql30="call update_history_lack()"
    #cur.execute(sql30)
    #conn.autocommit(True)
    
    print("ok,start next task!")
    sql30="call update_history_lack()"
    cur.execute(sql30)
    conn.autocommit(True)
    print("all task fineshed!")
    cur.close()
    conn.close()
    
def timerFun(sched_Timer):
    flag=0
    while True:
        now=datetime.datetime.now()
        if now==sched_Timer:
            run_Task1()
            flag=1
        else:
            if flag==1:
                sched_Timer=sched_Timer+datetime.timedelta(minutes=80)
                #把minutes=1改成hours=1就变成了每个小时定时任务，改成days=1就变成电
                #每天的定时任务
                flag=0
if __name__=='__main__':
    ##定义从什么时候开始执行
    sched_Timer=datetime.datetime(2017,8,29,10,21,10)
    print 'run the timer task at {0}'.format(sched_Timer)
    timerFun(sched_Timer)
                                           