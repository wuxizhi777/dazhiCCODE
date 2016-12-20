#-*- coding: UTF-8 -*-
import requests
import logging
import ConfigParser

import threading
import time
import os
import json
import re
import operator


from collections import deque

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filemode='w+')
# create threadLock
threadLock=threading.Lock()  #1、创建一个锁

print('#####################*********************************')
def logConf():
    '''
    :return:  无，配置 log 打印
    '''
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='test.log',
                        filemode='w')


    logging.info('info message  debug effor')




def readConf():
    """
    #从配置文件中读出相应的参数
    :return:
    """
    cf = ConfigParser.ConfigParser()
    cf.read('conf.cfg')
    log_path=cf.get('config','log_path')
    return log_path

def find_SQL_Project_from_Log(log_path):
    '''

    :param log_path:   文件的路径
    :return:  [{},{},{}]  从日志中读出的 sql 块
    '''
    files_path=log_path
    all_queris=[]
    files_list=get_files_list(files_path)
    print files_path
    if(len(files_path)):
        files_list=choose_one_file_1(files_list)   #  choose the file name

        for one_file in files_list:
            all_queris.extend(get_queries(one_file))  # have wrong fix me!!!

    return all_queris

def find_SQL_from_OneFilename(log_path,filename):
    all_queris=[]
    all_queris=get_queries(log_path+filename)

    return all_queris

def CleanSql_set(all_queris):
    '''
    del the repeat sql
    :param all_queris:[{},{},{},....]
    :return: my_queris: [{},{},{},...]
          my_dict: { sql:{}}.{ key:value}.{}.{}
          my_dict: one sql map to her logs
          sql 的去重，并且 一个sql 对应一个sqls 块
    '''
    my_queris=[]
    my_dict={}

    for one in all_queris:
        value=one.get('SQL','')
        if(value==''):
            pass
        my_dict[value]=one

    my_queris=my_dict.values()

    return my_queris,my_dict


def choose_one_file(files_list):
    mylist=[]
    i=1
    mydict={}
    print 'file names as follows :'
    for one in files_list:
       mydict[i]=one
       print  i,' :',one
       i=i+1
    print 'please input a file name: '
    print 'if you do not choose any file ,the program will choose all files in Folder named inputLog'
    key_number=raw_input()
    value=mydict.get(int(key_number),0)
    if value!=0:
        mylist.append(value)
    else:
        mylist=files_list[:]  #shallow copy



    return mylist

def choose_one_file_1(files_list):
    mylist=[]
    mydict={}
    mylist1=[]
    # filter   left the file format as kylin_query.log.2016-12-06
    for eachfile in files_list:

        if re.match(r'(.*)kylin_query\.log\.(.*)',eachfile):
            mylist1.append(eachfile)
            logging.info(eachfile)

    print 'file names as follows :'
    for one in mylist1:
       key=re.sub(r'\D',"",one[-26:])
       mydict[key]=one
       print  key,' :',one[-26:]

    print 'please choose function,'
    print 'input one letter:  \n'
    print '####a.  choose one file   ####### b.Select a period of time\n'
    print '\n'
    print '####c. one recent week   ######## d. one recent month       \n'
    # 对字典的关于key来排序
    list_dict_key=sorted(mydict.keys())
    print 'the date is form',list_dict_key[0],'to',list_dict_key[-1]

    choose_letter=raw_input()
    print choose_letter
    while True:
        if((choose_letter=='a')|(choose_letter=='b')|(choose_letter=='c')|(choose_letter=='d')):
            break
        print 'pls input letter , a or b or c,or d  .....'
        choose_letter=raw_input()
    print choose_letter
    if(choose_letter=='a'):  # choose one file
        key_number=input_number(list_dict_key)
        value = mydict.get(key_number, 0)
        if value != 0:
            mylist.append(value)
        else:
            mylist = files_list[:]  # shallow copy
################################################################################
    if(choose_letter=='b'): #choose a period file
        tempList=[]
        print 'the date is form', list_dict_key[0], 'to', list_dict_key[-1]
        print 'input the start date file index  the form as \'20161101\':'
        key_number1=input_number(list_dict_key)
        print 'input the end date file index:'
        key_number2=input_number(list_dict_key)
        #1. 对 dictionary 进行排序 并给出范围
        index1=list_dict_key.index(key_number1)
        #2. 输入的出错处理   获取到开始日期和结束日期
        index2=list_dict_key.index(key_number2)
        tempList=list_dict_key[index1:index2+1]
        #3.
        for key_number in  tempList:
            value = mydict.get(key_number, 0)
            if value != 0:
                mylist.append(value)
########################################################################################
    if( choose_letter=='c'):
        tempList=[]
        tempList=list_dict_key[-7:-1]
        for key_number in  tempList:
            value = mydict.get(key_number, 0)
            if value != 0:
                mylist.append(value)

    if (choose_letter == 'd'):
        tempList = []
        tempList = list_dict_key[-30:-1]
        for key_number in tempList:
            value = mydict.get(key_number, 0)
            if value != 0:
                mylist.append(value)

    return mylist

def input_number(list_dict_key):
    print 'pls  a file  index: '
    key_number = raw_input()
    # 对key_number 进行正则匹配，确保他是  20161206 号这种形式。而且在文件的范围中。

    while True:
        if re.match(r'20\d{6}', key_number):
            if (key_number >= list_dict_key[0]) & (key_number <= list_dict_key[-1]):
                break
        else:
            key_number = raw_input()

    return key_number

def get_special_queries(dict_list, key, value):
    """
    根据特殊的键值对,得到相应的dict数据
    :param dict_list: dict数据的集合
    :param key: 键
    :param value: 值
    :return: 对应的dict的集合 [{},{},{},{}]
    """
    return [block_dict for block_dict in dict_list if get_value_by_key(block_dict, key) == value]









def filterSQL_Project(all_queris):
    '''
    :param all_queris:  all useful imformation
    :return:   list of    ['sql ','project']
    这里 可以过滤条件
    '''


    filtername_Key='Success'
    filtername_Value='true'

    filtername_Key2 = 'Project'
    filtername_Value2 = 'GRO'

    filtername_Key3 = 'Realization Names'
    filtername_Value3 = '[smd_lex_path_sd]'

    List_temp=[block_dict for  block_dict in all_queris if get_value_by_key(block_dict,filtername_Key)==filtername_Value]
    #List_temp1=[block_dict for  block_dict in List_temp if get_value_by_key(block_dict,filtername_Key2)==filtername_Value2]
    List_temp_last=[block_dict for  block_dict in List_temp if get_value_by_key(block_dict,filtername_Key3)==filtername_Value3]



    mylist=[] # return [[],[],[],[sql,project],[][],....]
    mylist_temp=[]
    for one in List_temp_last:
        mylist_temp.append(one.get('SQL',''))
        mylist_temp.append(one.get('Project',''))
        logging.info("dazhi")
        logging.info(mylist_temp)
        mylist.append(mylist_temp)
        mylist_temp=[]


    return mylist


def get_files_list(path):
    '''
    :param path:  file_path such as base_path+r'/inputLog/'
    :return: [../../inputLog/XXX.log1,../../inputLog/XXX.log2]
    '''
    if os.path.exists(path):
        return [path+file_name for file_name in os.listdir(path)]
    else:
        return []

def get_queries(one_file):
    '''
    :param one_file:
    :return: allsqls form one_file [{},{},{}，{}]
    '''
    #init()
    datas=[]
    mylist=[]
    mydict={}
    ###
    pattern1 = 'Message:'
    pattern2 = '==========================[QUERY]==============================='
    print one_file
    with open(one_file) as f:
        for line,prelines in search(f,pattern1,pattern2,14):
            for pline in prelines:

                mylist.append(pline.strip())

            mylist.append(line.strip())
            mydict=list_to_dict(mylist)
            mylist=[]
            datas.append(mydict)

    return datas

def list_to_dict(mylist):

    mydict={}
    for one in mylist:
        templist= one.split(':',1)

        if(len(templist)==2):
            mydict[templist[0]]=templist[1].strip()


    return mydict

def search(lines,pattern1,pattern2,history=13):
    previous_lines=deque(maxlen=history)
    pre_line=''
    for li in lines:
        if (pattern2 in li) and (pattern1 in pre_line): #fix me!!!
            yield li,previous_lines
        pre_line=li
        previous_lines.append(li)

def get_value_by_key(dic, key):
    """
    得到某key的value值
    :param dic: dict
    :param key: key
    :return: value
    """

    return dic.get(key, 0)

def restfulApiSQL(listSQL_Project,url,usename,password,sql_dict):
    print " %s" % (time.ctime(time.time()))
    logging.info(time.ctime(time.time()))
    successNumber = 0  # 成功的请求次数
    duration_time = 0  # 所有查询时间的总和

    print 'the number of request is ',len(listSQL_Project)

    for one in  listSQL_Project:
        sql=''
        project_name=''
        if(len(one)==2):

            sql=one[0]
            project_name=one[1]

            logging.info(sql +'##'+project_name)
            str_true="true"
        params = {"sql": sql,
                  "offset": 0,
                  "limit": 50000,
                  "acceptPartial": True,
                  "project": project_name,
                  "backdoorToggles": {
                      "DEBUG_TOGGLE_DISABLE_QUERY_CACHE": str_true,
                  }
                 }
        print 'json.dumps:',json.dumps(params)
        response = requests.post(url + 'query', data=json.dumps(params),
                                 headers={'Content-Type': 'application/json;charset=UTF-8'},
                                 auth=(usename, password))
        print 'response.status_code: ',response.status_code
        print 'response.text: ',response.text
        if (response.status_code == 200):
            decodejson = json.loads(response.text)
            print(decodejson['duration'])
            duration_time = duration_time + int(decodejson['duration'])
            successNumber = successNumber + 1
        else:
            # 1. write respose.status_code,respose.test
            # 2.  the context in old log
            # 3. thread mutex
            if threadLock.acquire(True):  #2、获取锁状态，一个线程有锁时，别的线程只能在外面等着
                try:
                    fp = open('sqlFail_log.log', 'a+')  # write flower the end of  file
                    fp.write('response.status_code:'+str(response.status_code) + '\n')
                    fp.write(str(response.text) + '\n')
                    fp.write('sql: '+str(sql)+'\n' )
                    fp.write('the detailed info : ')
                    temp_one_dict=sql_dict.get(sql,'')
                    for one in temp_one_dict:
                        print ( one +': '+temp_one_dict[one])
                        fp.write(one +': '+temp_one_dict[one] +'\n')
                    fp.write('############################################')
                    fp.write('\n')
                finally:
                    fp.close()

                threadLock.release()  # 3、释放锁

    if (successNumber == 0):
        logging.error('restAPI all fall')
        return 0, 0
    else:
        duration_avg = duration_time / successNumber
    print 'successNumber  in  restfulApiSQL:',successNumber
    return successNumber, duration_avg

def div_list(ls,n):
    '''
     divide list to  n segment,every segment have the same length
    :param ls:  list  type:list
    :param n:    int   type:int
    :return:
    '''
    if not isinstance(ls,list) or not isinstance(n,int):
        return []
    ls_len = len(ls)
    if n<=0 or 0==ls_len:
        return []
    if n > ls_len:
        return []
    elif n == ls_len:
        return [[i] for i in ls]
    else:
        j = ls_len/n
        k = ls_len%n
        ### j,j,j,...(前面有n-1个j),j+k
        #步长j,次数n-1
        ls_return = []
        for i in xrange(0,(n-1)*j,j):
            ls_return.append(ls[i:i+j])
        #算上末尾的j+k
        ls_return.append(ls[(n-1)*j:])
        return ls_return

class RequestThread(threading.Thread):
    def __init__(self,listSQL_Project,url,usename,password,sql_dict):
        super(RequestThread,self).__init__()
        self._url=url
        self.listSQL_Project=listSQL_Project
        self._usename=usename
        self._password=password
        self._successNumber=0
        self._duration_avg=0
        self._sql_dict=sql_dict

    def run(self):
        successNumber,duration_avg=restfulApiSQL(self.listSQL_Project,self._url,self._usename,self._password,self._sql_dict)
        print('the time is %s',time.ctime())
        print ('_successNumber in func :',successNumber)
        self._successNumber =successNumber
        self._duration_avg=duration_avg

    def get_result(self):
        return self._successNumber

    def get_duration_avg(self):
        return  self._duration_avg

def delLogFile():
    if os.path.exists('sqlFail_log.log'):
        message = 'OK, the  file exists.'
        os.remove('test.log')
        os.remove('sqlFail_log.log')

    else:
        message = "Sorry, I cannot find the file named  sqlFail_log.log ,I will create it"


    print message
    return

def sendRestfulAPI(listSQL_Project,thread_number,url, usename, password, sql_dict,all_queris):
    type_number = len(listSQL_Project)
    if (len(listSQL_Project) < 50):
        listSQL_Project = listSQL_Project * 100

    each_listSQL_Project = div_list(listSQL_Project, thread_number)
    logging.info(len(each_listSQL_Project))

    for one in each_listSQL_Project:
        for tmp in one:
            print tmp

    i = 0
    start_time = time.time()
    successNumber = 0  # the return sql is right
    duration_avg = 0  # the avg time
    duration_total = 0  # temp
    threads = []
    while i < thread_number:
        t = RequestThread(each_listSQL_Project[i], url, usename, password, sql_dict)
        i = i + 1
        t.start()
        threads.append(t)

    # wait all threads finish
    for t in threads:
        t.join()

    for t in threads:
        successNumber = successNumber + t.get_result()

    for t in threads:
        duration_total = duration_total + t.get_duration_avg()

    if (successNumber > 0):
        duration_avg = duration_total / thread_number
        duration_avg = duration_avg / float(1000)

    print '\n'
    print '####################################\n'
    print  'the number of all sqls  in log is: ', len(all_queris)
    print  'the number of the restfulAPI which we send  is: ', len(listSQL_Project)
    print 'the types of sql is ', type_number
    print  'successNumber: ', successNumber
    print 'duration_avg: ', duration_avg, 's'

    end_time = time.time()
    print 'the cost time of the  mul thread   for  restfulAPI is:', end_time - start_time, 's'

def change (listSQL_Project,testProjectName):
    ListSQL_Project_withoutGreed=[]
    for one in listSQL_Project:
        tmp=[]
        tmp.append(one[0])
        tmp.append(testProjectName)
        ListSQL_Project_withoutGreed.append(tmp)

    return ListSQL_Project_withoutGreed


def getSetQueries(all_queris,key):
    '''

    :param all_queris:[{},{},{},{}]   sql 块
    :return:  cubenames []     返回 不重复的cube names
    '''
    cubenames=[]
    for one in all_queris:

        value=one.get(key,'')
        #print 'value',value
        if(value==''):
            pass
        cubenames.append(value)
    cubenames=list(set(cubenames))


    return cubenames

def printlist( inputlist):
    for one in inputlist:
        print one

def get_special_queries(dict_list, key, value):
    """
    根据特殊的键值对,得到相应的dict数据
    :param dict_list: dict数据的集合
    :param key: 键
    :param value: 值
    :return: 对应的dict的集合 [{},{},{},{}]
    """
    return [block_dict for block_dict in dict_list if get_value_by_key(block_dict, key) == value]


def get_sum(dict_list, key):
    """
    根据某个键,得到集合中所有该键的value的总和
    :param dict_list:
    :param key:
    :return:
    """
    return sum([eval(get_value_by_key(block_dict, key)) for block_dict in dict_list])

def get_avg(dividend, divisor):
    """
    得到平均值
    :param dividend: 被除数
    :param divisor: 除数
    :return:
    """
    if divisor == 0:
        return 0
    return dividend/divisor

def get_ordered_dicts1(queries,key):
    '''

    :param queries:   [{},{},{},{},{}]
    :param key:     key='Duration'
    :return:  sort list[]   from min to max
    '''
    list=[]
    for one in queries:
        list.append(float(one[key]))
    list.sort()
    return list

def get_special_loc_info1(dict_list, loc_rate):
    """
    根据指定的位置的百分比(1-100), 得到该处的信息
    :param dict_list:
    :param loc_rate:
    :return:
    """
    if loc_rate <= 0:
        loc_rate = 1
    elif loc_rate >= 100:
        loc_rate = 99
    if len(dict_list) == 0:
        return 0.0

    return dict_list[int((len(dict_list)-1) * loc_rate / 100)]
