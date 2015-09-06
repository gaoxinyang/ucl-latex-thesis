__author__ = 'gaoxinyang'
__author__ = 'gaoxinyang'
import pandas as pd
import sys
import numpy as np
import operator
import random
from collections import Counter
from math import sqrt
from random import shuffle
import math


def getsecondfeature(frame_train_count):
    columns1= ['log_date', 'log_time_hour', 'client_id', 'placement_id',
           'inventory_source_id', 'url', 'position_id', 'size', 'browser_id', 'os_id', 'user_agent',
           'screen_size_id', 'visited_domains', 'visited_logpoints', 'clicker']
    
    count_valid = {}
    second = {}
    second_valid = {}
    second_whole = []
#for column in columns1:
#    count[column] = frame_train_count.groupby(column).size()
#    count_valid[column] = [key for key in count[column].keys() if count[column][key] > 10000]

    for idx1, val1 in enumerate(columns1):
        for idx2, val2 in enumerate(columns1[idx1+1:]):
            second[val1] = frame_train_count.groupby([val1, val2])[val1].count()
            second_valid[val1] = [key for key in second[val1].keys() if second[val1][key] > 10000]
            second_whole.extend(second_valid[val1])
            second_whole.append(val1+':other')
    return second_whole

def notlast(itr):
    itr = iter(itr)  # ensure we have an iterator
    prev = itr.next()
    for item in itr:
        yield prev

        prev = item
print 'start'
file = sys.argv[1]
file = int(float(file))
cnt = Counter()
ctrcount = Counter()
pd.options.display.float_format = '{:.25f}'.format

columns = ['log_date', 'log_time_hour',
           'inventory_source_id', 'position_id', 'size', 'browser_id', 'os_id', 'user_agent',
           'screen_size_id', 'visited_domains', 'visited_logpoints', 'clicker', 'click_count']


columns1= ['log_date', 'log_time_hour', 'client_id', 'placement_id',
           'inventory_source_id', 'domain', 'url', 'position_id', 'size', 'browser_id', 'os_id', 'user_agent',
           'screen_size_id', 'visited_domains', 'visited_logpoints', 'clicker']

path_train_train = '%d/train.train.txt' % file
path_train_count = '%d/train.count.txt' %file
path_test_test = '%d/test.test.txt' % file
path_test_valid = '%d/test.valid.txt' % file



fo_train = open('%d/train.bi.txt' %file, 'w')
fo_test_test = open('%d/test.bi.test.txt' % file, 'w')
fo_test_valid = open('%d/test.bi.valid.txt' % file, 'w')
fo_index = open('%d/index.txt' % file, 'w')

#frame_train = pd.read_csv(path_train,dtype=str)
#frame_test =  pd.read_csv(path_test,dtype=str)

####################################################33clean it ######################

frame_train_train = pd.read_csv(path_train_train,dtype=str,error_bad_lines = False)
frame_train_count = pd.read_csv(path_train_count,dtype=str,error_bad_lines = False)
frame_test_test = pd.read_csv(path_test_test,dtype=str,error_bad_lines = False)
frame_test_valid = pd.read_csv(path_test_valid,dtype=str,error_bad_lines = False)
total_length = len(frame_train_count)
dict_unique = {}
# print frame_train[:10]
# frame_train_count = frame_train_count.head(10)
# print count['creative']
length = 0
frame_train_count = pd.DataFrame(frame_train_count, columns=columns)

# a = []
# b = []
#
# for index, row in frame_train_count.iterrows():
#     if row['user_country_id'] == str(208):
#         a.append(row['click_count'])
#     else:
#         b.append(row['click_count'])
#
# tally = Counter()
# for elem in a:
#     tally[elem] += 1
# print tally
#
# tally = Counter()
# for elem in b:
#     tally[elem] += 1
# print tally

for c_index in range(0, len(columns) - 1):
    # for c_index in range(0,1):
    seri_train_count = frame_train_count.ix[1:len(frame_train_count)-1, c_index]

    # x = seri_train_count.tolist()
    # tally=Counter()
    # for elem in x:
    #  tally[elem] += 1
    # print tally
    seri_train_count = seri_train_count[~seri_train_count.isnull()]
    unique_s = seri_train_count.unique()
    #print unique_s

    unique_s = unique_s.tolist() + ['other']
    # unique_s = [unique_s, 'other']
    index = 0
    unique_list = []
    dict_u = {}

    for u in range(0, len(unique_s)):
        dict_u[unique_s[u]] = u + length
    dict_unique[columns[c_index]] = dict_u
    length = length + len(unique_s)
index = 0
record_length = 0
for column in notlast(columns):
    index = index + 1
    dict_u = dict_unique[column]
    record_length = record_length + len(dict_u)
    for key in dict_u:
        fo_index.write(str(index) + ':' + str(key) + ' ' + str(dict_u[key]))
        fo_index.write('\n')
record_length = record_length + 100
record_length = 2155314


        # ctr[column] = (frame_train_ctr.groupby(column).size()/float(count[column])).fillna(0)
for index, row in frame_train_train.iterrows():
    if row[-1] == str(0) or row[-1] == 0:
        fo_train.write(str(0))
    else:
        fo_train.write(str(1))

    for column in notlast(columns):
        dict_u = dict_unique[column]
        if row[column] not in dict_u:
            id = dict_u['other']
            fo_train.write(' ' + str(id) + ':' + str(1))
        else:
            id = dict_u[row[column]]
            fo_train.write(' ' + str(id) + ':' + str(1))






    fo_train.write('\n')
fo_train.close()

for index, row in frame_test_test.iterrows():
    if row[-1] == str(0) or row[-1] == 0:
        fo_test_test.write(str(0))
    else:
        fo_test_test.write(str(1))

    for column in notlast(columns):
        dict_u = dict_unique[column]
        if row[column] not in dict_u:
            id = dict_u['other']
            fo_test_test.write(' ' + str(id) + ':' + str(1))
        else:
            id = dict_u[row[column]]
            fo_test_test.write(' ' + str(id) + ':' + str(1))




    fo_test_test.write('\n')
fo_test_test.close()


