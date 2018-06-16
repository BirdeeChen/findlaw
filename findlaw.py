#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 16:26:38 2018

@author: farm
"""
#%%
import sys
sys.path.append('/home/farm/Birdee/NLPenv')
#导入必要的库
import pandas as pd
import jieba
import numpy as np
from sklearn.model_selection import train_test_split
import jieba.analyse as ana
from sklearn.feature_extraction.text import TfidfVectorizer
import re
from sklearn.svm import SVC
import threading
import MySQLdb
#%%
#定义一些常量，如文件目录，常变量等。
counts = 0
sourcefile = '/home/farm/Birdee/NLPenv/findlaw.txt' #训练数据集
usrdict = '/home/farm/Birdee/NLPenv/lawcell_extended.txt' #法律术语辞典
stopwords = '/home/farm/Birdee/NLPenv/stopwords.txt'
r = u'[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'#去除非中文字符
# updatequerry = "UPDATE train SET content = title WHERE content = 'thiscontentisempty!'"
getquerry = "SELECT content, classify FROM train WHERE classify NOT REGEXP '法律咨询$' ORDER BY RAND() LIMIT 100000"

host = '127.0.0.1'
user = 'root'
password = 'farm'
db = 'findlaw'
conn = MySQLdb.connect(host=host, user=user, passwd=password, db=db, charset='utf8')
df = pd.read_sql_query(getquerry, conn)
df.drop_duplicates(inplace = True)
jieba.load_userdict(usrdict)
ana.set_stop_words(stopwords)
#%%
    
def threaded_fenci(queue):
    fencilist = []
    MAX_THREADS = 10
    
    def fenci():
        global counts
        while queue:
            counts += 1
            if counts % 10000 == 0:
                print('fenci called {} times.'.format(counts))
            text = queue.pop(0)
            text = re.sub(r, ' ', text)
            fencilist.append(' '.join(ana.extract_tags(text, topK=50)))
    threads = []
    for _ in range(MAX_THREADS):
        thread = threading.Thread(target=fenci)
        thread.setDaemon(True)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    return fencilist
#读入数据库文件

#%%
print (df.classify.value_counts())

#%%
df['fenci'] = pd.Series(threaded_fenci(df.content.tolist()))
#%%
print (df.head())
#%%
df.classify = df.classify.astype('category') #给类别关键字编数字码
df['ylabel'] = df.classify.cat.codes
X_train_set, X_test_set, y_train_set, y_test_set = train_test_split(df.fenci, df.ylabel, random_state = 4, shuffle = True)
#%%
vectorizer = TfidfVectorizer()
#%%
X = vectorizer.fit_transform(X_train_set).toarray()
y = np.array(y_train_set)
#%%
#调用sciketlearn，作分析
clf = SVC()
clf.fit(X, y)
#%%
#对测试集做向量化
X_test = vectorizer.transform(X_test_set).toarray()
y_test = np.array(y_test_set)
#%%
#对测试集的预测表现打分
clf.score(X_test, y_test)
#%%
#调用scikitlearn，作分析预测

