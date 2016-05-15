# coding: utf-8

import os
import sys
import re
import urllib2
import json
import time
import multiprocessing
import threading
import socket
import commands
import MySQLdb
from datetime import datetime

from lib.zip import *
from lib.log import *

g_proxy_ua = {'User-Agent':'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36','Accept-Language':'zh-CN,zh;q=0.8'}

gUrl = 'http://www.bilibili.com/video/'

gLogger = get_logger('bili.LOG', INFO)

gCreateTable = '''CREATE table if NOT EXISTS %s.%s(`qt` int NOT NULL PRIMARY KEY AUTO_INCREMENT,`av` int NOT NULL,title nvarchar(1024),`desc` nvarchar(10240),`img` nvarchar(1024),`au` char(128),`videodate` char(128),`videosource` int not null DEFAULT 1) DEFAULT CHARACTER SET utf8'''

# return type: None
def MatchType(value, valueType, errmsg = 'PARAM'):
    '''MatchType: match value with valueType'''

    if not isinstance(value, valueType):
        gLogger.die("[%s's type mismatch]: expect: %s vs actual: %s" %
                    (errmsg, str(valueType), type(value)))

# return type: list
def MatchUrl(prefix, *suffixList):
    '''MatchUrl: multi match prefix with suffix'''

    prefix = prefix if prefix.endswith('/') else prefix + '/'
    return map(lambda suffix: prefix + suffix, suffixList)

# return type: list
def MatchBiliTopPageUrlWithIndex(*indexList):
    '''MatchBiliTopPageUrlWithIndex, just use in this py for match bilibili url'''

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

# return type: bool
def ExpectEQ(x, y, errMsg = None):
    if x != y:
        gLogger.warning(errMsg if errMsg else "expect: '%s' vs actual '%s'" % (x, y))
        return False
    return True

# return type: None
def AssertNE(x, y, errMsg):
    if x == y:
        gLogger.die(errMsg if errMsg else "expect: '%s' != '%s'" % (x, y))

# return type: bool
def ExpectNE(x, y, errMsg = None):
    if x == y:
        gLogger.warning(errMsg if errMsg else "expect: '%s' vs actual '%s'" % (x, y))
        return False
    return True

# return type: None
def AssertGT(x, y, errMsg):
    if x <= y:
        gLogger.die(errMsg if errMsg else "expect: '%s' > '%s'" % (x, y))

# return type: None
def AssertGE(x, y, errMsg):
    if x < y:
        gLogger.die(errMsg if errMsg else "expect: '%s' >= '%s'" % (x, y))

class ErrorCode(object):
    '''ErrorCode: define all errorcode use in this .py'''

    __E_MAP = {}

    OK                      = 0
    UNKNOW_ERROR            = -1001

    INVALID_PARAM_ERROR     = -1002
    ASSERT_ERROR            = -1003
    SAVE_ERROR              = -1004
    MYSQL_NOT_READY         = -1005
    CREATE_DB_ERROR         = -1006
    CREATE_TABLE_ERROR      = -1007
    EXECUTE_SQL_ERROR       = -1008
    INSERT_VALUE_ERROR      = -1009

    __E_MAP[OK]                     = 'SUCCESS'
    __E_MAP[UNKNOW_ERROR]           = 'UNKNOW_ERROR'
    __E_MAP[INVALID_PARAM_ERROR]    = 'INVALID_PARAM_ERROR'
    __E_MAP[ASSERT_ERROR]           = 'ASSERT_ERROR'
    __E_MAP[SAVE_ERROR]             = 'SAVE_ERROR'
    __E_MAP[MYSQL_NOT_READY]        = 'MYSQL_NOT_READY'
    __E_MAP[CREATE_DB_ERROR]        = 'CREATE_DB_ERROR'
    __E_MAP[CREATE_TABLE_ERROR]     = 'CREATE_TABLE_ERROR'
    __E_MAP[EXECUTE_SQL_ERROR]      = 'EXECUTE_SQL_ERROR'
    __E_MAP[INSERT_VALUE_ERROR]     = 'INSERT_VALUE_ERROR'

    @staticmethod
    def ToString(errorcode):
        return ErrorCode.__E_MAP[errorcode]

# return type: str
def GetPage(url, timeout = 10, retry_times = 3):
    '''GetPage, visit url and download the page'''

    gLogger.debug('GetPage: url: %s' % url)

    proxy_url = urllib2.Request(url = url, headers = g_proxy_ua)

    content = ''
    times = 0
    error = None
    while times < retry_times:
        try:
            content = urllib2.urlopen(proxy_url, timeout = timeout).read()
            break
        except socket.timeout, ex:
            error = ex
        except urllib2.HTTPError, ex:
            error = ex
        except urllib2.URLError, ex:
            error = ex
        except Exception, ex:
            error = ex

        times += 1
        gLogger.warning('error: %s, retry[%s]: %d' % (repr(error), url, times))
        time.sleep(0.5)

    if content == '':
        gLogger.error('fail to GetPage: %s, %s' % (str(ex), url))

    return content

# return type: int
def GetPageCount(url):
    '''GetPageCount, simple get page count'''

    try:
        buf = GetPage(url)
        ret = re.search('共\s+?(\d+?)\s+?页', buf)
        return int(ret.group(1))
    except Exception as ex:
        gLogger.die(str(ex))

class PageInfo(object):
    '''PageInfo: store page urls range in [beginIndex, beginIndex + indexCount)'''

    def __init__(self, baseUrl = gUrl, beginIndex = 0, indexCount = -1):
        MatchType(baseUrl, str, 'baseUrl')
        MatchType(beginIndex, int, 'beginIndex')
        MatchType(indexCount, int, 'endIndex')

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
    '''Callback: service for thread-class'''

    def __init__(self):
        # private:
        self.__mResult = []
        self.__mDone = False
        self.__mErrcode = 0
        self.__mErrMsg = ''
        self.__mThread = None

    def SetThread(self, thread):
        self.__mThread = thread

    # public:
    def Run(self):
        self.__mDone = True

    def Wait(self):
        if self.__mThread == None:
            while not self.__mDone:
                time.sleep(0.5)
        else:
            self.__mThread.join()

    # private:
    def SetErrorCode(self, errcode, errmsg = ''):
        try:
            MatchType(errcode, int, 'errcode')
            MatchType(errmsg, str, 'errmsg')
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
        MatchType(result, list, 'result')
        self.__mResult = result

    def GetResult(self):
        return self.__mResult

class PageDownloader(threading.Thread):
    '''PageDownloader: download page which url in pageUrls'''

    def __init__(self, logger, pageUrls, cb):
        super(PageDownloader, self).__init__()

        MatchType(pageUrls, list, 'pageInfo')
        MatchType(cb, Callback, 'cb')

        self.mPageUrls = pageUrls
        cb.SetThread(self)
        self.mCb = cb
        self.mLogger = logger

    def run(self):
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
            self.mLogger.error(str(ex))
            self.mCb.SetErrorCode(ErrorCode.UNKNOW_ERROR, str(ex))
        finally:
            self.mCb.Run()

class VideoDetector(threading.Thread):
    '''VideoDetactor: search video in page'''

    def __init__(self, logger, pages, cb):
        super(VideoDetector, self).__init__()

        MatchType(pages, list, 'pages')
        MatchType(cb, Callback, 'cb')

        self.mPages = pages
        cb.SetThread(self)
        self.mCb = cb
        self.mLogger = logger

        self.mRegLi = re.compile(u'<li>.*?</li>', re.S)
        self.mRegKey = re.compile(u'<a href="/video/av(\d+?)/".*?title="(.*?)".*?<img data-img="(.*?)".*?>.*?<div class="v-desc">(.*?)</div>.*?<a class="v-author".*?>(.*?)</a>.*?<span class="v-date".*?>(.*?)</span>', re.S)

    def run(self):
        try:
            AssertNE(self.mPages, [], 'target pages is empty')

            result = []

            for page in self.mPages:
                # 1. split '<li>.*?</li>'
                ret = self.mRegLi.findall(page)

                # 2. filter 'ASMR'
                ret = filter(lambda item: 'ASMR' in item, ret)

                # 3. search key: [av, title, jpg, desc, author, date]
                ret = map(lambda item: self.mRegKey.search(item), ret)
                ret = map(lambda item: [item.group(i).strip() for i in range(1, 7)], ret)
                result.extend(ret)

            self.mCb.SetErrorCode(ErrorCode.OK)
            self.mCb.SetResult(list(result))
        except Exception as ex:
            self.mLogger.error(str(ex))
            self.mCb.SetErrorCode(ErrorCode.UNKNOW_ERROR, str(ex))
        finally:
            self.mCb.Run()

class SaveTargetInfo(object):
    '''SaveTargetInfo: base class for target save'''
    pass

class JsonInfo(SaveTargetInfo):
    '''JsonInfo: jsonFile, save result into the jsonFile'''

    def __init__(self, jsonFile):
        self.mJsonFile = jsonFile

    def __repr__(self):
        return 'json file name: %s' % self.mJsonFile

class DbInfo(SaveTargetInfo):
    '''DbInfo: host, user, passwd, db, port, table. But port not used'''

    def __init__(self, host = 'localhost', user = 'root', passwd = '', db = '', port = 3306, table = ''):
        self.mHost = host
        self.mUser = user
        self.mPasswd = passwd
        self.mDb = db
        self.mPort = port
        self.mTable = table

    def __repr__(self):
        return '[host: %s, user: %s, passwd: %s, db: %s, port: %s, table: %s]'\
               % (self.mHost, self.mUser, self.mPasswd, self.mDb, self.mPort, self.mTable)

class DbHandler(object):
    '''DbHandler: operate Db, all method return [errorcode, errMsg]'''

    def __init__(self, dbInfo):
        MatchType(dbInfo, DbInfo)

        self.mDbInfo = dbInfo
        self.mConnection = MySQLdb.connect(dbInfo.mHost, dbInfo.mUser, dbInfo.mPasswd, charset = 'utf8')

    def __del__(self):
        self.mConnection.commit()
        self.mConnection.close()

    def Execute(self, sql):
        ret = []
        cursor = None
        try:
            cursor = self.mConnection.cursor()

            begin = datetime.now()
            ret.append(cursor.execute(sql))
            end = datetime.now()

            if end.microsecond - begin.microsecond > 6000*1000: # > 6ms
                gLogger.warning('execute sql: %s, cost: %s' % (sql, end.microsecond - begin.microsecond))

            ret.append(cursor.fetchall())
            self.mConnection.commit()
        except cursor.Warning as ex:
            ret = [ErrorCode.EXECUTE_SQL_ERROR, 'sql: %s, err: %s' % (sql, str(ex))]
            self.mConnection.rollback()
        except cursor.Error as ex:
            ret = [ErrorCode.EXECUTE_SQL_ERROR, 'sql: %s, err: %s' % (sql, str(ex))]
            self.mConnection.rollback()
        except Exception as ex:
            ret = [ErrorCode.EXECUTE_SQL_ERROR, 'sql: %s, err: %s' % (sql, str(ex))]
            self.mConnection.rollback()
        finally:
            if cursor != None:
                cursor.close()

        gLogger.debug('execute sql: %s, ret: %s' % (sql, str(ret)))
        return ret

    def DbExist(self, db = None):
        dbName = db if db else self.mDbInfo.mDb
        sql = "show databases like '%s'" % dbName
        return (1 == self.Execute(sql)[0])

    def TableExist(self, db = None, table = None):
        dbName = db if db else self.mDbInfo.mDb
        tableName = table if table else self.mDbInfo.mTable

        self.Execute('use %s' % dbName)
        ret = self.Execute("show tables")

        return (tableName,) in ret[1]

    def CreateDb(self, db = None):
        dbName = db if db else self.mDbInfo.mDb

        sql = 'create database if not exists %s' % dbName
        ret = self.Execute(sql)
        if ErrorCode.EXECUTE_SQL_ERROR == ret[0]:
            ret[0] = ErrorCode.CREATE_DB_ERROR
        else:
            ret[0] = ErrorCode.OK

        return ret

    def CreateTable(self, db = None, table = None):
        dbName = db if db else self.mDbInfo.mDb
        tableName = table if table else self.mDbInfo.mTable

        global gCreateTable
        sql = gCreateTable % (dbName, tableName)
        ret = self.Execute(sql)
        if ErrorCode.EXECUTE_SQL_ERROR == ret[0]:
            ret[0] = ErrorCode.CREATE_TABLE_ERROR
        else:
            ret[0] = ErrorCode.OK

        return ret

    def MergeInsert(self, objList, db = None, table = None):
        dbName = db if db else self.mDbInfo.mDb
        tableName = table if table else self.mDbInfo.mTable

        # 1. load from db
        sql = 'SELECT av FROM %s.%s' % (dbName, tableName)
        ret = self.Execute(sql)
        if ErrorCode.EXECUTE_SQL_ERROR == ret[0]:
            return ret

        # 2. insert new item
        return self.Insert(objList, db, table, blacklist = [item[0] for item in ret[1]])

    # always return OK
    def Insert(self, objList, db = None, table = None, blacklist = []):
        dbName = db if db else self.mDbInfo.mDb
        tableName = table if table else self.mDbInfo.mTable

        # '\\': use as a normal char instead of '\' in SQL
        # '\"': use as a normal char instead of '"' in SQL
        # "\'": use as a normal char instead of "'" in SQL
        def SafeStringForSql(string):
            # the first \ use in python string, the second \ use in SQL string
            return string.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'")

        print len(blacklist)
        failCount = successCount = 0
        for obj in objList:
            if long(obj['av'], base = 10) not in blacklist:
                # value(av, title, desc, img, au, videodate)
                value = '''%s,"%s","%s","%s","%s","%s"''' % (obj['av'],\
                        SafeStringForSql(obj['title']), SafeStringForSql(obj['desc']),\
                        SafeStringForSql(obj['img']), SafeStringForSql(obj['au']),\
                        SafeStringForSql(obj['videodate']))
                sql = 'INSERT INTO %s.%s(`av`, `title`, `desc`, `img`, `au`, `videodate`) VALUE(%s)' %\
                      (dbName, tableName, value)
                ret = self.Execute(sql)
                if ErrorCode.EXECUTE_SQL_ERROR == ret[0]:
                    gLogger.error('error sql: %s, ret: %s' % (sql, str(ret)))
                    failCount += 1
                    continue
                successCount += 1

        gLogger.info('insert_new_count = %d, insert_fail_count = %d' % (successCount, failCount))

        return [ErrorCode.OK, '']

    @staticmethod
    def CheckMySQL():
        try:
            # check python-module MySQLdb
            import MySQLdb

            # check mysql-client
            ret = commands.getstatusoutput('mysql --version')
            if ret[0] != 0:
                raise Exception('mysql-client not found at localhost')

            # check mysql-server
            cmd = 'ps -ef | grep mysql | grep -v grep | wc -l'
            ret = commands.getstatusoutput(cmd)
            if ret[0] != 0:
                raise Exception('fail to run cmd: %s' % cmd)
            if int(ret[1].strip()) < 1:
                raise Exception('mysql-server not runing, '
                                'please start mysql-server: sudo service mysql start')

        except Exception as ex:
            return [ErrorCode.MYSQL_NOT_READY, str(ex)]

        return [ErrorCode.OK, '']

class ResultHandler(object):
    '''handle result with static method'''

    @staticmethod
    def Save(obj, target = JsonInfo('result.json')):
        MatchType(target, SaveTargetInfo, 'target')

        ret = ErrorCode.OK
        if isinstance(target, JsonInfo):
            ret = ResultHandler.SaveAsJson(obj, target)
        elif isinstance(target, DbInfo):
            ret = ResultHandler.SaveIntoDb(obj, target)
            if ErrorCode.OK != ret:
                tmpJsonFile = 'tmp_%s.json' % str(datetime.now()).replace(' ', '_')
                gLogger.warning('save obj as json: %s' % tmpJsonFile)
                ret = ResultHandler.SaveAsJson(obj, JsonInfo(jsonFile = tmpJsonFile))
        else:
            gLogger.warning('unknow target type: %s' % str(target))
            ret = ErrorCode.SAVE_ERROR

        return ret

    @staticmethod
    def SaveAsJson(obj, jsonInfo):
        MatchType(jsonInfo, JsonInfo, 'jsonInfo')

        try:
            with open(jsonInfo.mJsonFile, 'w') as f:
                json.dump(obj, f, indent = 4, separators = (',', ':'),
                          ensure_ascii = False, sort_keys=True)
        except Exception as ex:
            gLogger.die('fail to SaveAsJson: obj = %s, errMsg: %s' % (str(obj), repr(ex)))
            return ErrorCode.SAVE_ERROR

    @staticmethod
    def SaveIntoDb(obj, dbInfo):
        MatchType(dbInfo, DbInfo, 'dbInfo')

        ret = ErrorCode.OK
        try:
            db = DbHandler(dbInfo)

            # 1. check mysql
            ret = DbHandler.CheckMySQL()
            if ErrorCode.OK != ret[0]:
                raise Exception('fail to CheckMySQL, errMsg: %s' % str(ret))

            # 2. check db
            if not db.DbExist():
                gLogger.warning('check db: %s not exist, create a new db !!!' % dbInfo.mDb)
                ret = db.CreateDb()
                if ErrorCode.OK != ret[0]:
                    raise Exception('fail to create db: %s, errMsg: %s' % (dbInfo.mDb, str(ret)))

            # 3. check table
            if not db.TableExist():
                gLogger.warning('check table: %s.%s not exist, create new table !!!'
                                % (dbInfo.mDb, dbInfo.mTable))
                ret = db.CreateTable()
                if ErrorCode.OK != ret[0]:
                    raise Exception('fail to create table: %s.%s, errMsg: %s' % (dbInfo.mDb, dbInfo.mTable, str(ret)))

            # 4. merge insert into db
            # merge result, just only insert new item into db
            ret = db.MergeInsert(obj)
            if ErrorCode.OK != ret[0]:
                raise Exception('fail to Save obj into Db, errMsg: %s' % str(ret))

        except Exception as ex:
            gLogger.error('fail to SaveIntoDb: %s, errMsg: %s' % (str(dbInfo), str(ex)))

        return ret[0]

def Init():
    handler_encoding = urllib2.build_opener(ContentEncodingProcessor())
    urllib2.install_opener(handler_encoding)

def TaskFunc(pageInfo, threadCount):
    gLogger.info('START: process [%d] with threadCount [%d], deal with: %s'
                 % (os.getpid(), threadCount, pageInfo.ToString()))

    #logger = get_logger('bili.LOG.%d' % os.getpid())
    logger = gLogger

    start = datetime.now()

    cbList = []

    pageCount = len(pageInfo.mPageUrls)
    dealPagesPerTask = pageCount / threadCount
    releasePages = pageCount % threadCount

    beginIndex = 0
    for index in range(min(threadCount, pageCount)):
        pageCount = dealPagesPerTask
        if releasePages > 0:
            pageCount += 1
            releasePages -= 1

        cb = Callback()
        pageDown = PageDownloader(logger, pageInfo.mPageUrls[beginIndex:beginIndex + pageCount], cb)
        pageDown.start()
        cbList.append(cb)

        beginIndex += pageCount

    result = []
    for cb in cbList:
        cb.Wait()
        ExpectEQ(ErrorCode.OK, cb.GetErrorCode(), cb.GetErrorMsg())
        result += cb.GetResult()

    cb = Callback()
    searchKey = VideoDetector(logger, result, cb)
    searchKey.start()
    cb.Wait()
    ExpectEQ(ErrorCode.OK, cb.GetErrorCode(), cb.GetErrorMsg())

    gLogger.info('END: process [%d], deal with: %s, cost: %s'
                 % (os.getpid(), pageInfo.ToString(), datetime.now() - start))

    return cb.GetResult()

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

        result.append(pool.apply_async(func = TaskFunc, args = (pageInfo, 4)))

    pool.close()
    pool.join()

    gLogger.info('download and deal cost: %s' % (datetime.now() - start))

    res = []
    for item in result:
        res += item.get()

    # value(av, title, desc, img, au, videodate)
    res = map(lambda item: {'av': item[0], 'title': item[1],
                            'img': item[2], 'desc': item[3],
                            'au': item[4], 'videodate': item[5]}, res)

    print len(res)
    gLogger.info('get target count = %d' % len(res))

    start = datetime.now();
    # host, user, passwd, db, port, table
    ResultHandler.Save(res, target = DbInfo(host = 'localhost', user = 'root',
                                            passwd = 'caft', db = 'video', table = 'asrm'))
    gLogger.info('save result cost: %s' % (datetime.now() - start))

    return 0

if __name__ == '__main__':
    sys.exit(main())

    # following for test
    with open('test.json', 'r') as f:
        buf = json.load(f)

    print len(buf)
    ResultHandler.Save(buf, target = DbInfo(host = 'localhost', user = 'root',
                                            passwd = 'caft', db = 'video', table = 'asrm'))
