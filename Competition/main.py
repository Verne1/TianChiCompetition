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
    :return: 2014-12-17这一天的表
    """
    collection = get_collection('user_action_table')
    return_table = []
    count = 0
    for record in collection.find():
        if '2014-12-17 00' <= record['time'] < '2014-12-18 00':
            return_table.append(record)
            print count
        count += 1
    return return_table


def get_one_week_data():
    """
    :return: 返回2014-12-18前一周的数据表（2014-12-11 to 2014-12-17）
    """
    collection = get_collection('user_action_table')
    return_table = []
    count = 0
    for record in collection.find():
        if '2014-12-11 00' <= record['time'] < '2014-12-18 00':
            return_table.append(record)
            print count
        count += 1
    return return_table


def user_item_pairs(one_week_table):
    """
    :param one_week_table: 一周的user action数据表
    :return: 一周数据中不重复的user_item pairs
    Step1:遍历一周的数据，找出所有的user_id item_id 对
    Step2:取出重复pairs
    Step3:去除item_table没有涉及的item_id的pairs
    """
    # Step1
    user_item_pairs_list = []
    for record in one_week_table:
        user_item_pairs_list.append([record['user_id'], record['item_id']])
    # Step2
    list(set(user_item_pairs_list))
    # Step3
    collection = get_collection('items_table')
    need_delete_index = []  # 存放需要删除的pairs的下标
    count = 0
    for pair in user_item_pairs_list:
        if collection.find_one({'item_id': pair[1]}) == None:
            need_delete_index.append(count)
        count += 1
    for index in need_delete_index:
        user_item_pairs_list.remove(user_item_pairs_list.__getitem__(index))

    return user_item_pairs_list


def store_to_file():
    """
    :return:
    描述：参函数将2014-12-17，及2014-12-18前一周的数据分别存入文件（缓存）
    步骤：
    Step1:保存2014-12-17当日数据子集
    Step2:保存2014-12-18前一周数据子集
    """
    # Step1
    file_write = open(r'E:/12_17.txt', 'w')
    pickle.dump(get_one_day_data(), file_write)
    file_write.close()
    # Step2
    file_write = open(r'E:/a_week.txt', 'w')
    pickle.dump(get_one_week_data(), file_write)
    file_write.close()


def load_from_file(file_name):
    """
    :param file_name: 文件名和路径
    :return: list数据
    """
    file_read = open(file_name, 'r')
    return pickle.load(file_read)


store_to_file()

