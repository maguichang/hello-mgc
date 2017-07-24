# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 13:05:58 2017

@author: dell
"""

#定时执行一段代码,将task任务替换自己的程序
import platform
import os
import datetime
#待执行的任务
def run_Task():
    os_platfrom=platform.platform()
    if os_platfrom.startswith('Darwin'):
        print'this is mac os system'
        os.system('ls')
    elif os_platfrom.startswith('Window'):
        print'this is win system'
        os.system('dir')
def timerFun(sched_Timer):
    flag=0
    while True:
        now=datetime.datetime.now()
        if now==sched_Timer:
            run_Task()
            flag=1
        else:
            if flag==1:
                sched_Timer=sched_Timer+datetime.timedelta(minutes=1)
                #把minutes=1改成hours=1就变成了每个小时定时任务，改成days=1就变成电
                #每天的定时任务
                flag=0
if __name__=='__main__':
    sched_Timer=datetime.datetime(2017,7,24,11,40,00)
    print 'run the timer task at {0}'.format(sched_Timer)
    timerFun(sched_Timer)
