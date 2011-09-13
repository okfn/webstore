# http://code.google.com/p/pythonxy/source/browse/src/python/pytables/PLATLIB/tables/misc/lrucache.py?spec=svn.xy-27.d2ca019c1b156e5929b738c38644f6816dff377b&repo=xy-27&r=d2ca019c1b156e5929b738c38644f6816dff377b
# lrucache.py -- a simple LRU (Least-Recently-Used) cache class

# Copyright 2004 Evan Prodromou <evan@bad.dynu.ca>
# Licensed under the Academic Free License 2.1

# Modified to use monotonically increasing integer values as access times
# by Ivan Vilata i Balaguer <ivan@selidor.net>.

# arch-tag: LRU cache main module
import time
from heapq import heappush, heappop, heapify


class CacheKeyError(KeyError):
    pass

class LRUTimeoutCache(object):

    class __Node(object):

        def __init__(self, key, obj, timestamp):
            object.__init__(self)
            self.key = key
            self.obj = obj
            self.atime = timestamp

        def __cmp__(self, other):
            return cmp(self.atime, other.atime)

        def __repr__(self):
            return "<%s %s => %s (accessed at %s)>" % \
                   (self.__class__, self.key, self.obj, self.atime)

    def __init__(self, size, timeout=60*15):
        if size <= 0:
            raise ValueError, size
        elif type(size) is not type(0):
            raise TypeError, size
        object.__init__(self)
        self.__heap = []
        self.__dict = {}
        self.timeout = timeout
        self.size = size

    def __len__(self):
        return len(self.__heap)

    def __contains__(self, key):
        if self.__dict.has_key(key):
            if self.__dict[key].atime >= time.time() - self.timeout:
                return True
            else:
                del self.__dict[key]
        return False

    def __setitem__(self, key, obj):
        if key in self:
            node = self.__dict[key]
            node.obj = obj
            node.atime = time.time()
            heapify(self.__heap)
        else:
            while len(self.__heap) >= self.size:
                lru = heappop(self.__heap)
                del self.__dict[lru.key]
            node = self.__Node(key, obj, time.time())
            self.__dict[key] = node
            heappush(self.__heap, node)

    def __getitem__(self, key):
        if not key in self:
            raise CacheKeyError(key)
        else:
            node = self.__dict[key]
            node.atime = time.time()
            heapify(self.__heap)
            return node.obj

    def __delitem__(self, key):
        if key in self: 
            raise CacheKeyError(key)
        else:
            node = self.__dict[key]
            del self.__dict[key]
            self.__heap.remove(node)
            heapify(self.__heap)
            return node.obj

    def pop(self, key):
        if not key in self:
            raise CacheKeyError(key)
        else:
            node = self.__dict[key]
            del self.__dict[key]
            self.__heap.remove(node)
            heapify(self.__heap)
            return node.obj

    def __iter__(self):
        copy = self.__heap[:]
        while len(copy) > 0:
            node = heappop(copy)
            yield node.key
        raise StopIteration

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)
        if name == 'size':
            while len(self.__heap) > value:
                lru = heappop(self.__heap)
                del self.__dict[lru.key]

    def __repr__(self):
        return "<%s (%d elements)>" % (str(self.__class__), len(self.__heap))



