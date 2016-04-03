# -*- coding: utf-8 -*-
from pymongo import MongoClient
import logistic_regression as lr
import numpy as np
__author__ = 'Brown Wong'


def get_collection(collec_name):
    """
    :param collec_name: collection name, 即关系数据库中的表名
    :return: collection，即一张表
    """
    client = MongoClient('localhost', 27017)
    db = client.tianchi
    return db[collec_name]


def get_one_day_data():
    """
    描述：从user_table表中获取2014-12-17这一天的数据子集放入one_day_table表
    """
    user_table = get_collection('user_action_table')
    one_day_table = get_collection('one_day_tab;e')
    count = 0
    for record in user_table.find():
        if '2014-12-17 00' <= record['time'] < '2014-12-18 00':
            one_day_table.insert_one(record)
            print count
        count += 1


def get_one_week_data():
    """
    描述：从user_table表中获取2014-12-18前一周的数据子集放入one_week_table表
    """
    user_table = get_collection('user_action_table')
    one_week_table = get_collection('one_week_table')
    count = 0
    for record in user_table.find():
        if '2014-12-11 00' <= record['time'] < '2014-12-18 00':
            one_week_table.insert_one(record)
            print count
        count += 1


def user_item_pairs():
    """
    :return: 一周数据中不重复的user_item pairs
    Step1:遍历一周的数据，找出所有的user_id item_id 对
    Step2:去除重复pairs，并将数据暂存数据库
    Step3:去除item_table没有涉及的item_id的pairs,放入数据库另一张表
    """
    # Step1
    user_item_pairs_list = []
    one_week_table = get_collection('one_week_table')
    count = 0
    for record in one_week_table.find():
        print 'step1: '+str(int(100.0*count/6092231))+'%'
        user_item_pairs_list.append((record['user_id'], record['item_id']))
        count += 1
    # Step2
    user_item_pairs_list = list(set(user_item_pairs_list))
    list_len = len(user_item_pairs_list)
    user_item_pairs_table_temp = get_collection('user_item_pairs_table_temp')
    count = 0
    for pair in user_item_pairs_list:
        user_item_pairs_table_temp.insert_one({'user_id': pair[0], 'item_id': pair[1]})
        print 'step2: '+str(int(100.0*count/(list_len-1)))+'%'
        count += 1
    # Step3
    pairs_temp = get_collection('user_item_pairs_table_temp')
    items_table = get_collection('items_table')
    pairs = get_collection('user_item_pairs_table')
    count = 0
    for pair in pairs_temp.find():
        if items_table.find({'item_id': pair['item_id']}).count() != 0:
            pairs.insert_one(pair)
        count += 1
        print 'step3: '+str(int(100.0*count/2399169))+'%'


def target_matrix_empty():
    """
    :return:
    描述：
    根据数据库中的不重复的user item对生成空的目标矩阵，并保存到数据库。
    空是指X0，X1，X2，Label这四个属性为空
    """
    pairs = get_collection('user_item_pairs_table')
    target_matrix = get_collection('target_matrix_table')
    count = 0
    for pair in pairs.find():
        target_matrix.insert_one({'user_id': pair['user_id'], 'item_id': pair['item_id'],
                                  'X0': 0, 'X1': 0, 'X2': 0, 'label': 0})
        count += 1
        print 'step1: '+str(int(100.0*count/216755))+'%'


# target_matrix_empty()
def get_attr_value():
    """
    :return:
    描述：
    为X0，X1，X2，label属性赋值
    步骤：
    Step1：为X0赋值
    Step2：为X1赋值
    Step3：为X2赋值
    Step4为label赋值
    """

    # Step1,Step2
    target_matrix = get_collection('target_matrix_table')
    one_day_table = get_collection('one_day_table')
    count = 0
    for example in target_matrix.find():
        x0 = one_day_table.find({'user_id': example['user_id'], 'item_id': example['item_id'],
                                'behavior_type': 1}).count()
        x1 = one_day_table.find({'user_id': example['user_id'], 'item_id': example['item_id'],
                                'behavior_type': 3}).count()
        if x0 != 0 or x1 != 0:
            target_matrix.update_one({'user_id': example['user_id'], 'item_id': example['item_id']},
                                     {'$set': {'X0': x0, 'X1': x1}})
        count += 1
        print 'step1,2: '+str(int(100.0*count/216755))+'%'
    # Step3
    one_week_table = get_collection('one_week_table')
    count = 0
    for example in target_matrix.find():
        x2 = one_week_table.find({'user_id': example['user_id'], 'item_id': example['item_id'],
                                 'behavior_type': 4}).count()
        if x2 != 0:
            target_matrix.update_one({'user_id': example['user_id'], 'item_id': example['item_id']},
                                     {'$set': {'X2': 1}})
        count += 1
        print 'step3: '+str(int(100.0*count/216755))+'%'
    # Step4
    table_12_18 = get_collection('table_12_18')
    count = 0
    for example in target_matrix.find():
        label = table_12_18.find({'user_id': example['user_id'], 'item_id': example['item_id'],
                                 'behavior_type': 4}).count()
        if label != 0:
            target_matrix.update_one({'user_id': example['user_id'], 'item_id': example['item_id']},
                                     {'$set': {'label': 1}})
        count += 1
        print 'step4: '+str(int(100.0*count/216755))+'%'


# get_attr_value()
def train_weights():
    """
    :return: weights 参数，是一个1x4的向量
    步骤：
    Step1:从数据库中获取data_matrix,label_matrix矩阵
    Step2:求出weights
    Step3:绘出散点图和决策边界
    """
    # Step1
    data_matrix = []
    label_matrix = []
    target_matrix = get_collection('target_matrix_table')
    count = 0
    for example in target_matrix.find():
        data_matrix.append([1, example['X0'], example['X1'], example['X2']])
        label_matrix.append(example['label'])
        count += 1
        print 'step1: '+str(int(100.0*count/216755))+'%'
    # Step2
    weights = lr.gradient_ascent(data_matrix, label_matrix, max_cycles=3000)
    # Step3
    lr.plot_decision_boundary(weights, data_matrix, label_matrix)
    return weights


# print train_weights()
def predict():
    """
    :return:
    步骤：
    1.构造预测矩阵
    2.遍历18日数据，为X0赋值
    3.遍历18日数据，为X1赋值
    4.遍历11日至18日这八天的数据，为X2赋值
    5.预测，将结果赋值为label
    6.导出label为1的数据为csv文件
    """
    # Step1
    # pairs = get_collection('user_item_pairs_table')
    predict_matrix = get_collection('predict_matrix_table')
    '''
    count = 0
    for pair in pairs.find():
        predict_matrix.insert_one({'user_id': pair['user_id'], 'item_id': pair['item_id'],
                                  'X0': 0, 'X1': 0, 'X2': 0, 'label': 0})
        count += 1
        print 'step1: '+str(int(100.0*count/216755))+'%'
    '''
    # Step2,Step3
    table_12_18 = get_collection('table_12_18')
    '''
    count = 0
    for example in predict_matrix.find():
        x0 = table_12_18.find({'user_id': example['user_id'], 'item_id': example['item_id'],
                              'behavior_type': 1}).count()
        x1 = table_12_18.find({'user_id': example['user_id'], 'item_id': example['item_id'],
                              'behavior_type': 3}).count()
        if x0 != 0 or x1 != 0:
            predict_matrix.update_one({'user_id': example['user_id'], 'item_id': example['item_id']},
                                      {'$set': {'X0': x0, 'X1': x1}})
        count += 1
        print 'step2,3: '+str(int(100.0*count/216755))+'%'
    '''
    # Step4
    '''
    one_week_table = get_collection('one_week_table')
    count = 0
    for example in predict_matrix.find():
        x2 = one_week_table.find({'user_id': example['user_id'], 'item_id': example['item_id'],
                                 'behavior_type': 4}).count() + table_12_18.find({'user_id': example['user_id'],
                                                                                  'item_id': example['item_id'],
                                                                                  'behavior_type': 4}).count()
        if x2 != 0:
            predict_matrix.update_one({'user_id': example['user_id'], 'item_id': example['item_id']},
                                      {'$set': {'X2': 1}})
        count += 1
        print 'step3: '+str(int(100.0*count/216755))+'%'
    '''
    # Step5
    weights = train_weights()
    count = 0
    for example in predict_matrix.find():
        label = lr.classify([1, example['X0'], example['X1'], example['X2']], weights)
        if label == 1:
            predict_matrix.update_one({'user_id': example['user_id'], 'item_id': example['item_id']},
                                      {'$set': {'label': 1}})
        count += 1
        print 'step5: '+str(int(100.0*count/216755))+'%'
    print weights

#predict()
weights = train_weights()