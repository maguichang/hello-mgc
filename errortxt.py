<<<<<<< HEAD
# -*- coding: utf-8 -*-
# author:maguichang time:2018/3/22
import datetime
import time
def main():
    # fname = 'errors.txt'
    a, b = 3, 0
    c = a / b
    return c
if __name__ == '__main__':
    fname = 'errors.txt'
    err_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    try:
        result = main()
    except Exception as e:
        print(repr(e))
        with open(fname, 'a+') as f:#'w',open参数表示写入，写入之前把源文件内容清空。'a+'，表示追加
            f.write('main()'+'\n'+repr(e)+'\n'+err_time+'\n')#需要添加异常发生的时间，是哪个函数发生了异常等详细信息
        print('save error message to {}'.format(fname))
    else:
        print(result)

=======
# -*- coding: utf-8 -*-
# author:maguichang time:2018/3/22
import datetime
import time
def main():
    # fname = 'errors.txt'
    a, b = 3, 0
    c = a / b
    return c
if __name__ == '__main__':
    fname = 'errors.txt'
    err_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    try:
        result = main()
    except Exception as e:
        print(repr(e))
        with open(fname, 'a+') as f:#'w',open参数表示写入，写入之前把源文件内容清空。'a+'，表示追加
            f.write('main()'+'\n'+repr(e)+'\n'+err_time+'\n')#需要添加异常发生的时间，是哪个函数发生了异常等详细信息
        print('save error message to {}'.format(fname))
    else:
        print(result)

>>>>>>> 5c1e5155c5d3a7acc4552c2ccba740687bcd2a06
