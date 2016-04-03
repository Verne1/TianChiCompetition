# -*- coding: utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt
__author__ = 'Brown Wong'


def load_dataset(file_name):
    data_matrix = []
    label_matrix = []
    fr = open(file_name)
    for line in fr.readlines():
        line_list = line.strip().split()
        data_matrix.append([1.0, float(line_list[0]), float(line_list[1])])
        label_matrix.append(int(line_list[2]))
    return data_matrix, label_matrix


def sigmoid(x):
    """
    :param x: X可以是实数，也可以是矩阵，向量
    :return:
    """
    return 1.0/(1+np.exp(-x))


def gradient_ascent(data_matrix, label_matrix, alpha=0.001, max_cycles=500):
    """
    :param data_matrix: 这里并不要求输入的是matrix类型，数组类型即可
    :param label_matrix:
    :param alpha:
    :param max_cycles:
    :return: 权值/参数，是一个向量。类型为np.matrix
    Description:Batch Gradient Ascent!
    """
    data_matrix = np.mat(data_matrix)
    label_matrix = np.mat(label_matrix).transpose()
    m, n = np.shape(data_matrix)
    weights = np.ones((n, 1))
    for k in range(max_cycles):
        h = sigmoid(data_matrix*weights)
        error = (label_matrix-h)
        weights = weights + alpha*data_matrix.transpose()*error
    return weights


def stochastic_grad_ascent(data_matrix, label_matrix, num_iter=150):
    """
    :param data_matrix:
    :param label_matrix:
    :param num_iter:
    :return: 权值向量,类型为np.array
    描述：每次迭代随机选择一个example，计算误差并更新权值参数
    """
    m, n = np.shape(data_matrix)
    weights = np.ones(n)
    for j in range(num_iter):
        for i in range(m):
            alpha = 4/(1.0+j+i)+0.01
            rand_index = int(np.random.uniform(0, m))
            h = sigmoid(np.sum(data_matrix[rand_index]*weights))
            error = label_matrix[rand_index]-h
            weights += alpha*error*data_matrix[rand_index]
    return weights


def plot_decision_boundary(weights, data_matrix, label_matrix):
    """
    :param weights: 权值/参数，是np.matrix类型的向量。
    :param data_matrix:
    :param label_matrix:
    :return:
    功能：画出数据的散点图和决策边界
    """
    # 加载数据和变量定义
    weights = weights.getA()  # 将matrix类型转化为array类型
    data_matrix = np.array(data_matrix)  # 将list类型转化为array类型
    examples_num = np.shape(data_matrix)[0]
    xcord1 = []  # 存放第一类点的横坐标
    ycord1 = []
    xcord2 = []  # 存放第二类点的纵坐标
    ycord2 = []
    # 存放坐标
    for i in range(examples_num):
        if int(label_matrix[i]) == 1:
            xcord1.append(data_matrix[i, 2])  # 更改这里的值改变视图的维度
            ycord1.append(data_matrix[i, 3])
            print ''
        else:
            #xcord2.append(data_matrix[i, 2])
            #ycord2.append(data_matrix[i, 3])
            print ''
    # 绘图
    figure = plt.figure()
    ax = figure.add_subplot(111)
    ax.scatter(xcord1, ycord1, s=10, c='red', marker='x')
    ax.scatter(xcord2, ycord2, s=10, c='green')
    x = np.arange(0, 20.0, 0.1)
    y = (-weights[0]-weights[2]*x)/weights[3]
    ax.plot(x, y)
    plt.xlabel('X1')
    plt.ylabel('X2')
    plt.show()


def classify(input_vector, weights, threshold=0.5):
    prob = sigmoid(sum(input_vector*weights))
    if prob > threshold:
        return 1
    else:
        return 0

