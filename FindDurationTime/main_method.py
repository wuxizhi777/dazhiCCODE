# -*- coding: utf-8 -*-
from func  import  *
import time

if __name__=='__main__':
   print 'start'
   duration='Duration'

   logConf()  # config the log
   #1. readconf
   log_path=readConf()
   #2. choose file   and present    choose the function
   allqueries=find_SQL_Project_from_Log(log_path)   #其实是增加了 天数的限制
   #  allsqls form one_file [{},{},{}，{}]
   #3.列出cube,project

   print 'pls input  a  letter , C(ube) or P(roject) .....  '
   choose_letter = raw_input()
   print choose_letter
   while True:
      if ((choose_letter == 'C') | (choose_letter == 'P')):
         break
      print 'pls input letter , C or P .....'
      choose_letter = raw_input()
   print 'You choose the letter: ',choose_letter


   filterKey='Realization Names'
   if(choose_letter=='C'):
      cube_key = 'Realization Names'
      cubeNames_set = getSetQueries(allqueries, cube_key)  # 返回不重复的 cubenames
      print 'cube_name as follows:'
      printlist(cubeNames_set)
      print 'pls input the cube_name you want to find: '
      value_choose=raw_input()
      value_choose='['+value_choose+']'

   if(choose_letter=='P'):
      filterKey ='Project'
      pro_key = 'Project'
      proNames_set = getSetQueries(allqueries, pro_key)  # 返回不重复的 projectnames
      print 'project_name as follows:'
      printlist(proNames_set)
      print 'pls input the project_name you want to find: '
      value_choose=raw_input()

   lastqueries=get_special_queries(allqueries,filterKey,value_choose)  #得到最后的查询块
   #day_ordered = get_ordered_dicts(lastqueries, duration, False)
   durationTime=get_ordered_dicts1(lastqueries,duration)
   success_count = len(lastqueries)
   sum_response_time = get_sum(lastqueries, duration)
   avg_response_time = get_avg(sum_response_time, success_count)
   print 'avg_durationTime95',avg_response_time
   durationTime95= get_special_loc_info1(durationTime,95)
   durationTime98= get_special_loc_info1(durationTime, 98)
   print 'durationTime95',durationTime95
   print 'durationTime98',durationTime98
   print 'end'

   # 4.choose one cube 算出duration time