import re
import threading
import time
import socket
import datetime
import requests
import http.client
import csv
import MySQLdb
from scraper0507 import Downloader
from scraper0507 import RedisCache
from bs4 import BeautifulSoup
from random import random, choice
from multiprocessing import Pool
from redis import StrictRedis

socket.setdefaulttimeout(10)
MAX_THREADS = 1
proxyapi = 'http://120.25.150.39:8081/index.php/api/entry?method=proxyServer.generate_api_url&packid=7&fa=1&qty=1&time=1&pro=&city=&port=1&format=txt&ss=1&css=&dt=1'
seedurl = 'http://china.findlaw.cn/ask/browse/'


def sqlquery(query, values = None):
    '''
    This function runs the sql Query sentences.
    Parameters:
    query (str): The sql query to be runned.
    values (tuple or list of tuple): the values to be passed, default to None.
    '''

    host = 'localhost'
    user = 'root'
    password = 'farm'
    db = 'findlaw'

    try:
        conn = MySQLdb.connect(host = host, user = user, passwd = password, db = db, charset = 'utf8')
        cur = conn.cursor()
        cur.execute('USE {}'.format(db))
        if values:
            cur.executemany(query, values)
        else:
            cur.execute(query)
        conn.commit()
        conn.close()
    except MySQLdb.Error as e:
        print ('MySQL error:', e)
    result = cur.fetchall()
    return result

def getcrawlqueue(query):
    '''
    This function generates the crawl_queue, can be called many times.
    '''
    return [url[0] for url in sqlquery(query)]

def getproxy(proxyapi):
    return {'http': 'http://{}'.format(requests.get(proxyapi).text)}

def content_links(D, url):#用什么下载器，下载哪个url
    code = D(url, bsparser='getContent')
    # print (code)
    return code

def mp_crawler(D, crawl_queue):
    pool = Pool()
    while len(crawl_queue):
        url = crawl_queue.pop()
        # code = pool.apply_async(content_links, (D, url)).get()
        pool.apply_async(content_links, (D, url))
        # if code in [403, 407]:
        #     crawl_queue.push(url)
        #     break
    pool.close()
    pool.join()
    # return code

def getValues(cache):
    value = []
    for url in cache.client.scan_iter():
        res = cache[url]
        result = res['html']
        if result and res['code'] == 200:
            value.append(((re.search('[0-9]+', res['url'])[0]), res['url'], result['title'],
                         result['content'], result['date'], result['classify'], res['code']))
    return value

class RedisQueue:
    '''
    RedisQueue stores ursl to crawl to Redis
    Parameters:
    client: a Redis client connected to the key-value database for the webcrawling cache
    db (int): which database to use for Redis
    '''
    def __init__(self, client = None, db = 1, queue_name = 'url'):
        self.client = (StrictRedis(host = 'localhost', port = 6379, db = db) if client is None else client)
        self.name = queue_name
        self.seen_set = 'seen: %s' % queue_name

    def __len__(self):
        return self.client.llen(self.name)

    def push(self, element):
        '''Push an element to the tail of the queue'''
        if isinstance(element, list):
            element = [e for e in element]
            self.client.lpush(self.name, *element)
        else:
            self.client.lpush(self.name, element)

    def pop(self):
        '''Pop an element from the head of the queue'''
        return self.client.rpop(self.name).decode('utf-8')
    
    def erase(self):
        return self.client.flushdb()

if __name__ == '__main__':
    target_size = sqlquery("SELECT COUNT(*) FROM questionlink")[0][0]
    print ('{} links remain crawling.'.format(target_size))
    crawl_size = 10000
    num_crawl = target_size // crawl_size
    getquery = "SELECT url FROM questionlink LIMIT {}".format(crawl_size)
    savequery = "INSERT INTO train (uniqueID, url, title, content, qtime, classify, htmlcode) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)"
    
    for i in range(num_crawl):
        urllis = getcrawlqueue(getquery)
        crawl_queue = RedisQueue()
        crawl_queue.erase()
        crawl_queue.push(urllis)

        print ('Round {}, Num of target links {}.'.format(i + 1, len(crawl_queue)))
        cache = RedisCache(db=2, compress=True)
        deletequery = "DELETE FROM questionlink WHERE id <= {}".format((i + 1) * crawl_size)
        # proxy = getproxy(proxyapi)
        proxy = None
        print (proxy)
        D = Downloader(cache=cache, proxies=proxy)
        code = mp_crawler(D, crawl_queue)
        while code in [403, 407]:
            # proxy = getproxy(proxyapi)
            D = Downloader(cache = cache, proxies = proxy)
            code = mp_crawler(D, crawl_queue)
        
        # print ('Saving to MySQL.')
        # sqlquery(savequery, values = getValues(cache))
        # print ('Saving job done.')
        # sqlquery(deletequery)
        # print ('Erasing Cache.')
        # cache.erase()
    
    
        
