# coding: utf-8

class cache(object):
    def __init__(self, cache_size = 10):
        self.__cache = {}
        self.__key = []
        self.cache_size = cache_size

    def has_key(self, key):
        return key in self.__key

    def __getitem__(self, key):
        self.touch(key)
        return self.__cache[key]

    def add(self, key, value):
        if key not in self.__key:
            if len(self.__cache) == self.cache_size:
                del self.__cache[self.__key[-1]]
                del self.__key[-1]
            self.__cache[key] = value
            self.__key.insert(0, key)

    def touch(self, key):
        if key in self.__key:
            self.__key.remove(key)
            self.__key.insert(0, key)
        else:
            raise AssertionError('not cache key: %s' % key)


