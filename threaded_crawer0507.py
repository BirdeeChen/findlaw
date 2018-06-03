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

socket.setdefaulttimeout(10)
MAX_THREADS = 4
proxyapi = 'http://120.25.150.39:8081/index.php/api/entry?method=proxyServer.generate_api_url&packid=7&fa=1&qty=1&time=1&pro=&city=&port=1&format=txt&ss=1&css=&dt=1'
seedurl = 'http://china.findlaw.cn/ask/browse/'


def sqlquery(query, values = None):
    '''
    This function runs the sql Query sentences.
    Parameters:
    query (str): The sql query to be runned.
    values (tuple or list of tuple): the values to be passed, default to None.
    '''

    host = '127.0.0.1'
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

def threaded_crawer(proxy = None, cache = RedisCache(db=0, compress=True)):
    '''
    This function does the threaded crawling job
    Parameters:
    proxy (dict): the proxy to used for crawling, default to None
    '''
    threads = []

    D = Downloader(cache=cache, proxies=proxy)
    
    def content_links():
        global crawl_queue
        print ('current thread: {}'.format(threading.current_thread()))
        while crawl_queue:
            url = crawl_queue.pop()
            if not url or 'http' not in url:
                continue
            code = D(url, bsparser='getContent')
            if code in [403, 407]:
                crawl_queue.append(url)
                break

    for _ in range(MAX_THREADS):
        # print('thread {} running.'.format(threading.get_ident()))
        thread = threading.Thread(target=content_links)
        thread.setDaemon(True)
        thread.start()
        threads.append(thread)
        
    # print ('{} threads created.'.format(len(threads)))
    print ('active threading: {}'.format(threading.active_count()))
    # if not thread.is_alive():
    #     threads.remove(thread)
    for thread in threads:
        thread.join()
        
def getValues(cache):
    value = []
    for url in cache.client.scan_iter():
        res = cache[url]
        result = res['html']
        if result and res['code'] == 200:
            value.append(((re.search('[0-9]+', res['url'])[0]), res['url'], result['title'],
                         result['content'], result['date'], result['classify'], res['code']))
    return value

if __name__ == '__main__':
    target_size = sqlquery("SELECT COUNT(*) FROM questionbackup")[0][0]
    print ('{} links remain crawling.'.format(target_size))
    crawl_size = 50
    num_crawl = target_size // crawl_size
    getquery = "SELECT url FROM questionbackup WHERE NOT EXISTS (SELECT url FROM train) LIMIT {}".format(crawl_size)
    savequery = "INSERT INTO train (uniqueID, url, title, content, qtime, classify, htmlcode) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)"
    
    
    for i in range(num_crawl):
        crawl_queue = getcrawlqueue(getquery)
        print ('Round {}, Num of target links {}.'.format(i + 1, len(crawl_queue)))
        cache = RedisCache(db=2, compress=True)
        deletequery = "DELETE FROM questionlink WHERE id <= {}".format((i + 1) * crawl_size)
        proxy = getproxy(proxyapi)
        # proxy = None
        
        while crawl_queue:
            print ('crawl_queue length', len(crawl_queue))
            try:
                resp = requests.get(seedurl, proxies=proxy, timeout = 5)
                if resp.status_code in [403, 407]:
                    time.sleep(1)
                    print ('Change ip proxy')
                    proxy = getproxy(proxyapi)
                    print (proxy)
                    threaded_crawer(proxy, cache = cache)
                else:
                    print (proxy)
                    threaded_crawer(proxy, cache = cache)
            except (requests.exceptions.RequestException,
                    requests.exceptions.ChunkedEncodingError,
                    requests.ConnectionError, http.client.IncompleteRead, http.client.HTTPException) as e:
                    time.sleep(1)
                    print ('Change ip proxy')
                    proxy = getproxy(proxyapi)
                    print (proxy)
                    threaded_crawer(proxy, cache = cache)
        print ('Saving to MySQL.')
        sqlquery(savequery, values = getValues(cache))
        print ('Saving job done.')
        # sqlquery(deletequery)
        # print ('Erasing Cache.')
        # cache.erase()
    
    
        
