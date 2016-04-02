# -*- coding: utf-8 -*-
from pymongo import MongoClient
import pickle
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
    one_day_table = get_collection('one_day_table')
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
    Step2:取出重复pairs
    Step3:去除item_table没有涉及的item_id的pairs
    Step4:将user_item_pairs存入数据库
    """
    # Step1
    user_item_pairs_list = []
    one_week_table = get_collection('one_week_table')
    count = 0
    for record in one_week_table.find():
        print str(int(count/6092231))+'%'
        user_item_pairs_list.append((record['user_id'], record['item_id']))
        count += 1
    # Step2
    list(set(user_item_pairs_list))
    list_len = len(user_item_pairs_list)
    # Step3
    collection = get_collection('items_table')
    need_delete_index = []  # 存放需要删除的pairs的下标
    count = 0
    for pair in user_item_pairs_list:
        if collection.find_one({'item_id': pair[1]}) == None:
            need_delete_index.append(count)
        count += 1
        print str(int(count/list_len))+'%'
    for index in need_delete_index:
        user_item_pairs_list.remove(user_item_pairs_list.__getitem__(index))
    # Step4
    user_item_pairs_table = get_collection('user_item_pairs_table')
    for pair in user_item_pairs_list:
        user_item_pairs_table.insert_one({'user_id': pair[0], 'item_id': pair[1]})


user_item_pairs()


