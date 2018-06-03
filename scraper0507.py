# -*- coding: utf-8 -*-
import requests, re, time, datetime, json, zlib, csv, http.client
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from random import choice, randrange
from datetime import timedelta
from redis import StrictRedis

seedurl = 'http://china.findlaw.cn/ask/'
#年月Page url地址格式：http://china.findlaw.cn/ask/d201803_page1/
#问题Page url地址格式：http://china.findlaw.cn/ask/question_43837794.html
urltest = 'http://chnaa.findlaw.cn/ask/browse/'

YEARS = [str(years) for years in range(2004, 2018)]
MONTHS = ['{:02}'.format(months) for months in range(1, 13)]
USER_AGENT = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:59.0) Gecko/20100101 Firefox/59.0'
PROXIES = {'http' : 'http://myproy.net:8888'}
# print (YEARS, MONTHS)

class Throttle:
    '''
    Add a delay between downloads to the same domain
    '''
    def __init__(self, delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        domain = urlparse(url).netloc
        last_accessed = self.domains.get(domain)
        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (time.time() - last_accessed)
            if sleep_secs > 0:
                # domain has been accessed recently (< delay), so need to sleep
                time.sleep(sleep_secs)
        # update the last accessed time
        self.domains[domain] = time.time()

class Downloader:
    # seedurl = 'http://china.findlaw.cn/ask/'
    """ Downloader class to use cache and requests for downloading pages.
        For contructor, pass:
        delay (int): # of secs delay between requests (default: 5)
        user_agent (str): user agent string (default: 'wswp')
        proxies (list[dict]): list of possible proxies, each
            must be a dict with http / https keys and proxy values
        cache (dict or dict-like obj): keys: urls, values: dicts with keys (html, code)
        bsparser (str): must be one of ['gettotalpages', 'getLinks', 'getContent']
    """

    def __init__(self, delay = 1, user_agent = USER_AGENT, cache = {}, proxies = None):
        # instance variables
        self.throttle = Throttle(delay)
        self.user_agent = user_agent
        self.cache = cache
        self.proxies = proxies

    def __call__(self, url, bsparser = None):
        """ Call the downloader class, which will return HTML from cache
            or download it
            args:
                url (str): url to download
            kwargs:
                num_retries (int): # times to retry if 5xx code (default: 2)
        """
        try:
            result = self.cache[url]
            if result:
                result.update({'url': url})
                print('Loaded from cache:', url, result['code'])

        except (KeyError, UnicodeDecodeError):
            result = None
        if result and result['code'] != 200 and result['code'] != 404:
            # server error so ignore result from cache
            # and re-download
            result = None
        if result and result['code'] == 200 and result['html'] and '法律咨询' in result['html']['classify']:
            #数据不完全，重新下载
            result = None
        if result is None:
            # result was not loaded from cache, need to download
            self.throttle.wait(url)
            headers = {'User-Agent': self.user_agent}
            result = self.download(url, headers, bsparser)
            self.cache[url] = result
        return result['code']

    def download(self, url, headers, bsparser):
        """ Download a and return the page content
            args:
                url (str): URL
                headers (dict): dict of headers (like user_agent)
                proxies (dict): proxy dict w/ keys 'http'/'https', values
                    are strs (i.e. 'http(s)://IP') (default: None)
                bsparser (str): method to parse html, must be one of ['gettotalpages', 'getLinks', 'getContent']
        """
        # print('Downloading:', url, 'with proxy IP {}'.format(self.proxies['http']))
        num_retries = 2
        print('Downloading:', url)
        try:
            resp = requests.get(url, headers = headers, proxies = self.proxies, timeout = 5)
            print ('Downloading status:', resp.status_code)
            html = resp.text
            if resp.status_code == 200:
                return {'html': self.applybs(html, bsparser), 'code': resp.status_code, 'url': url}
            elif resp.status_code == 404:
                return {'html' : None, 'code' : resp.status_code, 'url' : url}
            elif (resp.status_code == 408 or 500 <= resp.status_code < 600) and num_retries:
                num_retries -= 1
                print ('Retry download after 3 sec.')
                time.sleep(3)
                return self.download(url, headers, bsparser)
            elif resp.status_code == 403:
                print ('Retry download after 5 mins.')
                time.sleep(303)
                return self.download(url, headers, bsparser)
        except (requests.exceptions.RequestException,
                requests.exceptions.ChunkedEncodingError,
                requests.ConnectionError, http.client.IncompleteRead, http.client.HTTPException) as e:
            print('Download {} error:'.format(url), e)
            return {'html': None, 'code': 403, 'url': url}

    def getTotalpages(self, html):
        '''function that returns the total page of a specified year and month
            Args:
                html(html object):
            Returns:
                int or false message
        '''
        print('getTotalpages called @ {}'.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        if html is None:
            return None
        else:
            bsObj = BeautifulSoup(html, 'html.parser')
            try:
                lastpagetag = bsObj.find('a', string=u'尾页')
                if 'href' in lastpagetag.attrs:
                    urlprefix = re.search(re.compile(
                        '.*d[0-9]{6}_page'), lastpagetag.attrs['href'])
                    totalpages = re.search(re.compile(
                        '[0-9]+/$'), lastpagetag.attrs['href'])
                    if totalpages:
                        return [urlprefix.group() + str(link) for link in range(1, int(totalpages.group()[:-1]) + 1)]
            except AttributeError as e:
                print('Attribute Error in getTotalPages:', e)
                return None


    def getLinks(self, html):
        ''' This function return all the links to the specific question
            Args:
                html(object):
            Returns:
                set: The links in a set
        '''
        print('getLinks called @ {}'.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        pages = list()
        if html is None:
            pages = None
        else:
            bsObj = BeautifulSoup(html, 'html.parser')
            try:
                linklist = bsObj.find_all('a', class_='rli-item item-link')
                for link in linklist:
                    if 'href' in link.attrs:
                        pages.append(link.attrs['href'])
            except AttributeError as e:
                print('Attribute Error in getLinks:', e)
                pages = None
        return pages

    def abnormalchar(self, content):
        return ''.join([c if len(c.encode('utf-8')) < 4 else '?' for c in content])

    def getContent(self, html):
        ''' This function get the title/content/time and classify
            Args:
            html(str):
        Returns:
            dict: title, content, date, classify
        '''
        print('getContent called @ {}'.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        # contents = {}
        # statuscode = re.compile('tip-item tip-item-.')
        if html is None:
            contents = None
        else:
            bsObj = BeautifulSoup(html, 'html.parser')
            try:
                title = bsObj.find('h1', class_='q-title').get_text()
                content = bsObj.find('p', class_='q-detail').get_text()
                date = bsObj.find('span', class_='about-item').get_text()
                classify = bsObj.find('span', string = u'正文', class_ = 'loc-text loc-link').find_previous('a').get_text()
                contents = {'title': self.abnormalchar(title), 'content': re.sub(
                    r'\s+', '', self.abnormalchar(content)), 'date': date, 'classify': classify}
            except AttributeError as e:
                print('Atrribute Error in getContent:', e)
                contents = None
        return contents

    def applybs(self, html, bsparser):
        if bsparser in ['gettotalpages', 'getLinks', 'getContent']:
            return {'gettotalpages' : self.getTotalpages, 'getLinks' : self.getLinks, 'getContent' : self.getContent}[bsparser](html)
        else:
            raise KeyError('bsparser must be specified.')

class RedisCache:
    def __init__(self, client = None, encoding = 'utf-8', db = 0, compress = True):
        # if a client object is not passed then try
        # connecting to redis at the default localhost port
        self.client = StrictRedis(host = 'localhost', port = 6379, db = db) if client is None else client
        # self.expires = expires
        self.encoding = encoding
        self.compress = compress
        

    def __getitem__(self, url):
        '''
        Load value from Redis for the given URL
        '''
        record = self.client.get(url)
        if record:
            if self.compress:
                record = zlib.decompress(record)
            try:
                rec = record.decode(self.encoding)
            except UnicodeDecodeError:
                rec = bytes(json.dumps({'html' : None, 'code' : 403}), self.encoding)
            return json.loads(rec)
        else:
            raise KeyError(url + ' does not exist.')

    def __setitem__(self, url, result):
        '''
        Save value in Redis for the given URL
        '''
        data = bytes(json.dumps(result), self.encoding, errors = 'ignore')
        if self.compress:
            data = zlib.compress(data)
        self.client.set(url, data)

    def __len__(self):
        return self.client.dbsize()

    def erase(self):
        self.client.flushdb()




def page_links():
    start = time.time()
    D = Downloader(cache = RedisCache())
    for year in YEARS:
        for month in MONTHS:
            D(seedurl + 'd' + year + month, bsparser = 'gettotalpages')
            end = time.time()
            if end - start > 15:
                time.sleep(10)
                start = time.time()

def question_links():
    links_cache = RedisCache(db = 0)
    question_cache = RedisCache(db = 1)
    start = time.time()
    D = Downloader(cache=question_cache)
    for key in links_cache.client.scan_iter():
        if links_cache[key]['html'] is None:
            continue
        else:
            for link in links_cache[key]['html']:
                D(link, bsparser = 'getLinks')
                end = time.time()
                if end - start > randrange(8, 10):
                    time.sleep(randrange(10, 15))
                    start = time.time()

def content_links():
    question_cache = RedisCache(db = 1, compress = False)
    content_cache = RedisCache(db = 2, compress = False)
    start = time.time()
    D = Downloader(cache=content_cache)
    for key in question_cache.client.scan_iter():
        if question_cache[key]['html'] is None:
            continue
        else:
            for question in question_cache[key]['html']:
                #print (question)
                D(question, bsparser = 'getContent')
                #print (code)
                content = content_cache[question]['html']
                if content is None:
                    continue
                else:
                    # writer.writerow(content.update({'url' : question}))
                    end = time.time()
                    if end - start > randrange(8, 10):
                        time.sleep(randrange(10, 13))
                        start = time.time()

if __name__ == '__main__':
    # pass
    # page_links()
    # question_links()
    content_links()
    # D = Downloader(cache = RedisCache())
    # totalpages = D('http://china.findlaw.cn/ask/d201703', bsparser = 'gettotalpages')
    # print (len(totalpages))
    # D = Downloader()
    # links = D('http://china.findlaw.cn/ask/d201703_page3829', bsparser='getLinks')
    # print (len(links))
    # D = Downloader()
    # content = D('http://china.findlaw.cn/ask/question_38586569.html', bsparser = 'getContent')
    # print (content)
