# coding: utf-8

import os
import sys
import re
import urllib2
import json
from lib.zip import *
from lib.color import *

#gUrl = 'http://www.bilibili.com/video/music-video-1.html'
gUrl = 'http://www.bilibili.com/video/'

# begin: must is a num
# end: must is a num
# vec: must is a list or tuple
# deal range: [begin, end)
def ForEachDo(begin, end, vec, func):
    if not isinstance(begin, int):
        raise Exception("'begin'[%s] must is int, actual is " % (begin, type(begin)))
    if not isinstance(end, int):
        raise Exception("'end'[%s] must is int, actual is %s" % (end, type(end)))
    if not isinstance(vec, (list, tuple)):
        raise Exception("'vec'[%s] must is list or tuple, actual is %s" % (vec, type(vec)))
    if not callable(func):
        raise Exception("'func' must callable")

    for index in range(begin, end):
        func(vec[index])

def MatchBiliWithIndex(baseUrl = gUrl, index = 1):
    url = baseUrl if baseUrl.endswith('/') else baseUrl + '/'
    return url + 'music-video-%d.html' % index

def GetPage(url):
    try:
        ret = urllib2.urlopen(url, timeout = 5)
        buf = ret.read()
        return buf
    except Exception as ex:
        print 'Fatal: catch Exception: %s' % str(ex)

def GetPageCount(url):
    try:
        buf = GetPage(url)
        ret = re.search('共\s+?(\d+?)\s+?页', buf)
        return int(ret.group(1))
    except Exception as ex:
        return 0

class PageInfo(object):
    def __init__(self, baseUrl = gUrl, beginIndex = 1, endIndex = -1):
        self.mBaseUrl = baseUrl if baseUrl > 0 else 1
        self.mBeginIndex = beginIndex
        self.mEndIndex = endIndex

class TaskItem(object):
    def __init__(self):
        self.mDone = False

    def Run(self):
        self.mDone = True

class PageDownItem(TaskItem):
    def __init__(self, pageInfo = PageInfo()):
        super(PageDownItem, self).__init__()

        if not isinstance(pageInfo, PageInfo):
            raise Exception('PageInfo type not match')

        self.mPageInfo = pageInfo
        self.mPages = []

    def Run(self):
        if self.mPageInfo.mEndIndex == -1:
            self.mPageInfo.mEndIndex = GetPageCount(MatchBiliWithIndex(self.mPageInfo.mBaseUrl, 1))

        for index in range(self.mPageInfo.mBeginIndex, self.mPageInfo.mEndIndex + 1):
            url = MatchBiliWithIndex(self.mPageInfo.mBaseUrl, index)
            self.mPages.append(GetPage(url))

        super(PageDownItem, self).Run()

        print len(self.mPages)

def Init():
    handler_encoding = urllib2.build_opener(ContentEncodingProcessor())
    urllib2.install_opener(handler_encoding)

def main(argv = sys.argv):
    Init()

    pageDown = PageDownItem(PageInfo(beginIndex = 1, endIndex = 20))
    pageDown.Run()

    return 0

if __name__ == '__main__':
    sys.exit(main())
