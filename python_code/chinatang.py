#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import re
from py2neo import Node, Relationship, Graph, NodeMatcher


def read_txt(path):
    cov_list=[]
    with open(path,encoding='utf-8') as f:
        tmp_file=f.readlines()
    for i in tmp_file:
        if i!= '\n':
            cov_list.append(i)
    return cov_list


class Covid():
    num=None  #病例编号
    adress='' #病例归属行政区
    track_event=[] #病例轨迹列表
#     date=''  #病例发布日期，这个数据没有解析
    def __init__(self,num ,adress, track_event):
        self.num = num
        self.adress = adress
        self.track_event = track_event


#从病例文本中解析病例数据，建立病例列表
def covid_people(cov_list,adress_set):
    people_list=[]  #把解析出来的病例全放在这个列表里
    
    for i in range(len(cov_list)):
        num=re.findall(r"\d+\.?\d*",cov_list[i][:10])[0]   #第49例现住路南区。近期主要活动轨迹。。。解析49
        if cov_list[i].find('现住')!= -1:       ##第49例现住路南区。近期主要活动轨迹。。。解析现住的地点，分两种情况分别处理
            adress=cov_list[i][cov_list[i].find('现住')+2:cov_list[i].find('。')]
        else:
            adress=cov_list[i][4:cov_list[i].find('。')]
        track_event=re.split('、|，|；',cov_list[i][cov_list[i].find('轨迹：')+3:-3])   #近期主要活动轨迹：路南南湖金地小区，高新盛华市场等。
        for i,_ in enumerate(track_event):
            if track_event[i]== '高新盛华市场':    #对部分地名进行归一化处理
                track_event[i]= '盛华市场'
        track_set.update(track_event)
    #     print(num, adress, track_event)
        people_list.append(Covid(num, adress, track_event))
    return people_list


def create_node_people(label,person,num, adress, track_event):
    '''创建病例node
    '''
    get_node=NodeMatcher(graph).match().where(name=person).first()
    if get_node:
        return get_node
    else:
        node_=Node(label[0],label[1],name=person,num = num ,adress = adress ,track_nums=0, track=track_event)
        graph.merge(node_,label[1],'name')
        return node_

def create_node_track(label, adress):
    '''创建track node
    '''
    get_node=NodeMatcher(graph).match().where(name=adress).first()
    if get_node:
        return get_node
    else:
        node_=Node(label,name=adress, num_of_related=0)
        graph.merge(node_,label,'name')
        return node_
    
def create_relationship(node1,node2,relationship):
    '''创建病例 track relationships
    '''
    r = Relationship(node1, relationship, node2)
    graph.merge(r)
    return r


def generate_covid_graph(people_list,people_type):
    '''
    people_list:病例列表，
    people_type:病例类型，confirmed,asymptomatic
    '''
    for person in people_list:
        person_node=create_node_people(['person',people_type],'第'+str(person.num)+'例',person.num, person.adress, person.track_event)

        for adress in  person.track_event:
            adress_node=create_node_track('track', adress)
            create_relationship(person_node,adress_node,'to')
            person_node['track_nums']=person_node['track_nums']+1
            adress_node['num_of_related']=adress_node['num_of_related']+1
            graph.push(person_node)
            graph.push(adress_node)


comfirmed=r'F:\temp\chinatang\comfirmed.txt'
asymptomatic=r'F:\temp\chinatang\asymptomatic.txt'


if __name__='main':
    #病例数据列表
    confirmed_list=read_txt(comfirmed)
    asymptomatic_list=read_txt(asymptomatic)

    #病例数据解析
    track_set=set() #把所有用户去过的地点都放在这个集合里
    confirmed_people=covid_people(confirmed_list,track_set)
    asymptomatic_people=covid_people(asymptomatic_list,track_set)
    
    #创建图
    graph = Graph("http://localhost:7474", name="covid", password="neo4jj")
    generate_covid_graph(confirmed_people,'confirmed')
    generate_covid_graph(asymptomatic_people,'asymptomatic')


#len(track_set)  可以用track_set的数量与neo4j中最终建成的track数量进行对比，判断是否有问题

























