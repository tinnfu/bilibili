# coding: utf-8

import os
import sys
import re
import urllib2
import json
import time
import multiprocessing
from datetime import datetime

from lib.zip import *
from lib.color import *
from lib.log import *

gUrl = 'http://www.bilibili.com/video/'
gLogger = get_logger('bili.LOG')

# return type: list
def MatchUrl(prefix, *suffixList):
    prefix = prefix if prefix.endswith('/') else prefix + '/'
    return map(lambda suffix: prefix + suffix, suffixList)

# return type: list
def MatchBiliTopPageUrlWithIndex(*indexList):
    global gUrl
    return map(lambda index: MatchUrl(gUrl, 'music-video-%d.html' % index)[0], indexList)

# return type: None
def AssertTrue(expr, errMsg = 'expect true vs actual false'):
    if not expr:
        gLogger.die(errMsg)

# return type: None
def AssertEQ(x, y, errMsg = None):
    if x != y:
        gLogger.die(errMsg if errMsg else "expect: '%s' vs actual '%s'" % (x, y))

# return type: None
def AssertNE(x, y, errMsg):
    if x == y:
        gLogger.die(errMsg if errMsg else "expect: '%s' != '%s'" % (x, y))

# return type: None
def AssertGT(x, y, errMsg):
    if x <= y:
        gLogger.die(errMsg if errMsg else "expect: '%s' > '%s'" % (x, y))

def AssertGE(x, y, errMsg):
    if x < y:
        gLogger.die(errMsg if errMsg else "expect: '%s' >= '%s'" % (x, y))

class ErrorCode(object):
    __E_MAP = {}

    OK                      = 0
    UNKNOW_ERROR            = -1001

    INVALID_PARAM_ERROR     = -1002
    ASSERT_ERROR            = -1003

    __E_MAP[OK]                     = 'SUCCESS'
    __E_MAP[UNKNOW_ERROR]           = 'UNKNOW_ERROR'
    __E_MAP[INVALID_PARAM_ERROR]    = 'INVALID_PARAM_ERROR'
    __E_MAP[ASSERT_ERROR]           = 'ASSERT_ERROR'

    @staticmethod
    def ToString(errorcode):
        return ErrorCode.__E_MAP[errorcode]

# return type: None
def TypeMatch(value, valueType, errmsg = 'PARAM'):
    if not isinstance(value, valueType):
        gLogger.die("[%s's type mismatch]: expect: %s vs actual: %s" %
                    (errmsg, str(valueType), type(value)))

# return type: str
def GetPage(url):
    for i in range(5):
        try:
            ret = urllib2.urlopen(url, timeout = 5)
            buf = ret.read()
            return buf
        except Exception as ex:
            gLogger.warn('%s: %s, retry: %d' % (str(ex), url, i))

    gLogger.die('GetPage: %s, %s' % (str(ex), url))

# return type: int
def GetPageCount(url):
    try:
        buf = GetPage(url)
        ret = re.search('共\s+?(\d+?)\s+?页', buf)
        return int(ret.group(1))
    except Exception as ex:
        gLogger.die(str(ex))

class PageInfo(object):
    def __init__(self, baseUrl = gUrl, beginIndex = 0, indexCount = -1):
        TypeMatch(baseUrl, str, 'baseUrl')
        TypeMatch(beginIndex, int, 'beginIndex')
        TypeMatch(indexCount, int, 'endIndex')

        AssertGT(beginIndex, 0,
                'invalid index: beginIndex:%d < 1' % beginIndex)

        if indexCount < 1:
            indexCount = GetPageCount(MatchBiliTopPageUrlWithIndex(1)[0])

        AssertGT(indexCount, 0,
                'invalid indexCount: indexCount:%d < 1' % indexCount)

        self.mPageUrls = MatchBiliTopPageUrlWithIndex(*[beginIndex + i for i in range(indexCount)])

        self.__mBeginIndex = beginIndex
        self.__mEndIndex = beginIndex + indexCount

    def ToString(self):
        return '[%d, %d)' % (self.__mBeginIndex, self.__mEndIndex)

class Callback(object):
    def __init__(self):
        # private:
        self.__mResult = []
        self.__mDone = False
        self.__mErrcode = 0
        self.__mErrMsg = ''

    # public:
    def Run(self):
        self.__mDone = True

    def Wait(self):
        while not self.__mDone:
            time.sleep(0.5)

    # private:
    def SetErrorCode(self, errcode, errmsg = ''):
        try:
            TypeMatch(errcode, int, 'errcode')
            TypeMatch(errmsg, str, 'errmsg')
            self.__mErrcode = errcode
            if errmsg != '':
                self.__mErrMsg = errmsg
            else:
                self.__mErrMsg = ErrorCode.ToString(errcode)
        except Exception as ex:
            self.__mErrcode = ErrorCode.UNKNOW_ERROR
            self.__mErrMsg = ErrorCode.ToString(errcode)

    def GetErrorCode(self):
        return self.__mErrcode

    def GetErrorMsg(self):
        return self.__mErrMsg

    def SetResult(self, result):
        TypeMatch(result, list, 'result')
        self.__mResult = result

    def GetResult(self):
        return self.__mResult

class PageDownItem(object):
    def __init__(self, logger, pageUrls, cb):
        TypeMatch(pageUrls, list, 'pageInfo')
        TypeMatch(cb, Callback, 'cb')

        self.mPageUrls = pageUrls
        self.mCb = cb
        self.mLogger = logger

    def Run(self):
        try:
            pages = []
            for url in self.mPageUrls:
                page = GetPage(url)
                AssertNE(page, '', 'get page is empty')
                pages.append(page)

            self.mCb.SetResult(pages)
            self.mCb.SetErrorCode(ErrorCode.OK)
        except AssertionError as ex:
            self.mLogger.error(str(ex))
            self.mCb.SetErrorCode(ErrorCode.ASSERT_ERROR, str(ex))
        except Exception as ex:
            self.mLogger.error(str(err))
            self.mCb.SetErrorCode(ErrorCode.UNKNOW_ERROR, str(ex))
        finally:
            self.mCb.Run()

class SearchKeyItem(object):
    def __init__(self, logger, pages, cb):
        TypeMatch(pages, list, 'pages')
        TypeMatch(cb, Callback, 'cb')

        self.mPages = pages
        self.mCb = cb
        self.mLogger = logger

        self.mRegLi = re.compile(u'<li>.*?</li>')
        self.mRegKey = re.compile(u'<a href="/video/av(\d+?)/".*?title="(.*?)".*?<img data-img="(.*?)".*?>.*?<div class="v-desc">(.*?)</div>.*?<a class="v-author".*?>(.*?)</a>.*?<span class="v-date".*?>(.*?)</span>')

    def Run(self):
        try:
            AssertNE(self.mPages, [], 'target pages is empty')

            result = []

            for page in self.mPages:
                # 1. split '<li>.*?</li>'
                ret = self.mRegLi.findall(page, re.S)

                # 2. filter 'ASMR'
                ret = filter(lambda item: 'ASMR' in item, ret)

                # 3. search key: [av, title, jpg, desc, author, date]
                ret = map(lambda item: self.mRegKey.search(item, re.S), ret)
                ret = map(lambda item: [item.group(i).strip() for i in range(1, 7)], ret)
                result.extend(ret)

            self.mCb.SetErrorCode(ErrorCode.OK)
            self.mCb.SetResult(list(result))
        except Exception as ex:
            self.mLogger.error(str(ex))
            self.mCb.SetErrorCode(ErrorCode.UNKNOW_ERROR, str(ex))
        finally:
            self.mCb.Run()

def Init():
    handler_encoding = urllib2.build_opener(ContentEncodingProcessor())
    urllib2.install_opener(handler_encoding)

def TaskFunc(pageInfo):
    gLogger.info('START: process [%d], deal with: %s'
                 % (os.getpid(), pageInfo.ToString()))

    #logger = get_logger('bili.LOG.%d' % os.getpid())
    logger = gLogger

    start = datetime.now()

    cb1 = Callback()
    pageDown = PageDownItem(logger, pageInfo.mPageUrls, cb1)
    pageDown.Run()
    cb1.Wait()
    AssertEQ(ErrorCode.OK, cb1.GetErrorCode(), cb1.GetErrorMsg())

    cb2 = Callback()
    searchKey = SearchKeyItem(logger, cb1.GetResult(), cb2)
    searchKey.Run()
    cb2.Wait()
    AssertEQ(ErrorCode.OK, cb2.GetErrorCode(), cb2.GetErrorMsg())

    gLogger.info('END: process [%d], deal with: %s, cost: %s'
                 % (os.getpid(), pageInfo.ToString(), datetime.now() - start))

    return cb2.GetResult()

def main(argv = sys.argv):
    Init()

    cpuCount = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(cpuCount)

    pageCount = GetPageCount(MatchBiliTopPageUrlWithIndex(1)[0])
    gLogger.info('GetPageCount = %d', pageCount)

    dealPagesPerTask = pageCount / cpuCount
    releasePages = pageCount % cpuCount
    result = []

    start = datetime.now()

    beginIndex = 0
    for index in range(min(cpuCount, pageCount)):
        pageCount = dealPagesPerTask
        if releasePages > 0:
            pageCount += 1
            releasePages -= 1

        pageInfo = PageInfo(gUrl, beginIndex + 1, pageCount)
        beginIndex += pageCount

        result.append(pool.apply_async(func = TaskFunc, args = (pageInfo,)))

    pool.close()
    pool.join()

    gLogger.info('cost: %s' % (datetime.now() - start))

    res = []
    for item in result:
        res += item.get()

    with open('result.json', 'w') as f:
        # [av, title, jpg, desc, author, date]
        res = map(lambda item: {'av': item[0], 'title': item[1],
                                'jpg': item[2], 'desc': item[3],
                                'author': item[4], 'date': item[5]}, res)
        json.dump(res, f, indent = 4, separators = (',', ':'), ensure_ascii = False, sort_keys=True)

    return 0

if __name__ == '__main__':
    sys.exit(main())
