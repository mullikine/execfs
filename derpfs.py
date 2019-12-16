#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-

# derpfs

# based on the tahoelafs project
# requires modification to pyfilesystem for mknod etc.
# but provides enough to extract a minimal linux install
# and then chroot into it :)
import pymongo
import gridfs
import os
import fs
import math
import fs.errors as errors
from fs.path import abspath, relpath, normpath, dirname, pathjoin
from fs.base import FS, NullFile
from fs import _thread_synchronize_default, SEEK_END
from fs.remote import RemoteFileBuffer
from fs.base import fnmatch, NoDefaultMeta
from posixpath import sep, pardir, join
import posixpath

import logging
from logging import DEBUG, INFO, ERROR, CRITICAL

import stat as statinfo

#from fs.memoryfs import MemoryFS
from fs.expose import fuse

logger = fs.getLogger('fs.derpfs')
logger.setLevel(DEBUG)

def _fix_path(func):
    """Method decorator for automatically normalising paths."""
    def wrapper(self, *args, **kwds):
        if len(args):
            args = list(args)
            args[0] = _fixpath(args[0])
        return func(self, *args, **kwds)
    return wrapper


def _fixpath(path):
    """Normalize the given path."""
    return abspath(normpath(path))


class DerpFS(FS):
    _meta = {	'virtual': False,
				'read_only': False,
				'unicode_paths': False,
				'case_insensitive_paths': False,
				'network': True,
                'derpfs.author': 'Derp Ltd.'
			}

    def __init__(self, mdb, collection_name):
        self.cache = False
        self.mdb = mdb
        self.collection_name = collection_name
        self.files = pymongo.collection.Collection(mdb, collection_name+".files")
        self.gfs = gridfs.GridFS(mdb, collection=collection_name)
        super(DerpFS, self).__init__(thread_synchronize=_thread_synchronize_default)

    def check_write(self):
        if self.getmeta('read_only'):
            raise errors.UnsupportedError('read only filesystem')
        return False

    @_fix_path
    def mknod(self, path, mode_no, dev_no):
        self.check_write()
        if not statinfo.S_ISCHR(mode_no):
            raise errors.UnsupportedError('Can only mknod character devices')

        bn = os.path.basename(path)
        dn = os.path.dirname(path)
        self.gfs.put('', filename=bn, type='dev', dev=dev_no, dir=dn, mode=mode_no)
        return 0

    @_fix_path
    def chmod(self, path, mode):
        self.check_write()
        info = self.getinfo(path)
        self.files.update({'_id':info['_id']}, {'$set':{'mode':mode}})

    @_fix_path
    def settimes(self, path, accessed_time, modified_time):
        self.check_write()
        """
        info = self.getinfo(path)
        self._log(DEBUG, "Setting times of '%s' to %s and %s" % (path,accessed_time, modified_time))
        if 'mtime' in info and info['mtime'] != modified_time:
            self.files.update({'_id':info['_id']}, {'$set':{'mtime':modified_time}})
        """

    @_fix_path
    def chown(self, path, uid, gid):
        self.check_write()
        info = self.getinfo(path)
        self.files.update({'_id':info['_id']}, {'$set':{'uid':uid,'gid':gid}})

    @_fix_path
    def symlink(self, path, to_path):
        self.check_write()
        try:
            info = self.getinfo(path)
            if info is not None:
                raise errors.DestinationExistsError(path)
        except errors.ResourceNotFoundError:
            pass
        bn = os.path.basename(path)
        dn = os.path.dirname(path)
        self.gfs.put('', filename=bn, type='link', target=to_path, dir=dn, mode=0777)

    @_fix_path
    def readlink(self, path):
        info = self.getinfo(path)
        if info['type'] != 'link':
            raise errors.UnsupportedError('cannot readlink')
        assert 'target' in info
        return info['target']

    @_fix_path
    def open(self, path, mode='r', **kwargs):
        if ('w' in mode or 'a' in mode):
            self.check_write()

        newfile = False
        if not self.exists(path):
            if 'w' in mode or 'a' in mode:
                newfile = True
            else:
                raise errors.ResourceNotFoundError(path)
        elif 'w' in mode:
            newfile = True

        if newfile:
            self.setcontents(path, '')
            handler = NullFile()
        else:
            info = self.getinfo(path)
            if info['type'] != 'file':
                self._log(DEBUG, "Path %s is  not a file" % path)
                raise errors.ResourceInvalidError(path)
            handler = self.gfs.get(info['_id'])

        return RemoteFileBuffer(self, path, mode, handler,
                    write_on_flush=False)

    @_fix_path
    def isfile(self, path):
        '''Essential'''
        return self.isdir(path) != True

    @_fix_path
    def isdir(self, path):
        '''Essential'''
        try:
            isdir = (self.getinfo(path)['type'] == 'dir')
        except errors.ResourceNotFoundError:
            isdir = False
        return isdir

    def listdir(self, *args, **kwargs):
        return [ item[0] for item in self.listdirinfo(*args, **kwargs) ]

    def listdirinfo(self, *args, **kwds):
        return list(self.ilistdirinfo(*args,**kwds))

    def ilistdir(self, *args, **kwds):
        for item in self.ilistdirinfo(*args,**kwds):
            yield item[0]

    def _info_from_node(self, node):
        info =  {'name': node['filename'],
                 'type': node['type'],
                 'created_time': node['uploadDate'],
                 'dir': node['dir'],
                 'md5': node['md5'],
                 '_id': node['_id'],
                 'st_mode': node['mode'],
                 'st_size': node['length'],
                 'size': node['length'],
                 'st_blksize': node['chunkSize'],
                 'st_blocks': int(math.ceil(node['length'] / node['chunkSize']))
                }
        if node['type'] == "dir":
            info['st_mode'] |= statinfo.S_IFDIR
            info['st_nlink'] = 2
        elif node['type'] == "link":
            info['st_mode'] |= statinfo.S_IFLNK
        elif node['type'] == 'file':
       	    info['st_mode'] |= statinfo.S_IFREG
            info['st_nlink'] = 1
        elif node['type'] == 'dev':
            #info['st_mode'] |= statinfo.S_IFCHR
            info['st_rdev'] = node['dev']
            info['st_nlink'] = 1

        if 'mtime' in node:
            info['modified_time'] = node['mtime']

        if node['type'] == 'link':
            info['target'] = node['target']
        return info

    @_fix_path
    def ilistdirinfo(self, path="/", wildcard=None, full=False, absolute=False,
                    dirs_only=False, files_only=False):
        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only can not both be True")

        if not path.endswith("/"):
            path += "/"
        dn = os.path.dirname(path)
        found = self.files.find({'dir':dn})
        for item in found:
            if dirs_only and item['type'] == 'file':
                continue
            elif files_only and item['type'] == 'dir':
                continue
            if item['filename'] == '':
                continue

            if wildcard is not None:
                if isinstance(wildcard,basestring):
                    if not fnmatch.fnmatch(item['filename'], wildcard):
                        continue
                else:
                    if not wildcard(item['filename']):
                        continue

            if full:
                item_path = relpath(pathjoin(path, item['filename']))
            elif absolute:
                item_path = abspath(pathjoin(path, item['filename']))
            else:
                item_path = item['filename']
            yield (item_path, self._info_from_node(item))

    @_fix_path
    def remove(self, path):
        '''Essential'''
        self.check_write()
        info = self.getinfo(path)
        if info['type'] != 'file' and info['type'] != 'link' and info['type'] != 'dev':
            raise errors.ResourceNotFoundError(path)
        self.gfs.delete(info['_id'])

    @_fix_path
    def removedir(self, path, recursive=False, force=False):
        '''Essential'''
        self.check_write()
    	info = self.getinfo(path)
        if info['type'] != 'dir':
            raise errors.ResourceInvalidError(path)
        # TODO: replace with 'find_one'
        if len(self.listdir(path)) > 1:
            raise errors.DirectoryNotEmptyError(path)
        self.gfs.delete(info['_id'])

    @_fix_path
    def setcontents(self, path, file, chunk_size=64*1024):
        self.check_write()

        info = None
        try:
            info = self.getinfo(path)
        except errors.ResourceNotFoundError:
            pass

        if info is not None:
            self.gfs.delete(info['_id'])
        else:
            info = {
                'name': os.path.basename(path),
                'dir': os.path.dirname(path),
                'type': 'file',
                'st_mode': 0644
            }
        self.gfs.put(file, filename=info['name'], type=info['type'], dir=info['dir'], mode=info['st_mode'])

    @_fix_path
    def makedir(self, path, dir_mode=0755, recursive=False, allow_recreate=False):
        '''Essential'''
        self.check_write()
        if recursive:
            raise errors.UnsupportedError('recursive not supported')
        bn = os.path.basename(path)
        dn = os.path.dirname(path)
        try:
            self.gfs.put('', filename=bn, type='dir', dir=dn, mode=dir_mode)
        except gridfs.errors.FileExists, e:
            if not allow_recreate:
                raise errors.DestinationExistsError(path)

    def rename(self, src, dst):
        '''Essential'''
    	raise errors.UnsupportedError('read only filesystem')

    def _log(self, level, message):
        if not logger.isEnabledFor(level): return
        print u'(%d) %s' % (id(self),
                                unicode(message).encode('ASCII', 'replace'))
        logger.log(level, u'(%d) %s' % (id(self),
                                unicode(message).encode('ASCII', 'replace')))

    @_fix_path
    def getinfo(self, path):
        '''Essential'''
        if self.cache and path in self.cache:
            return self.cache[path]
        bn = os.path.basename(path)
    	dn = os.path.dirname(path)
        node = self.files.find_one({'dir':dn,'filename':bn})
        if not node:
            raise errors.ResourceNotFoundError(path)

        info = self._info_from_node(node)
        if self.cache:
            if len(self.cache) > 1000:
                i = 0
                while i < 100:
                    self.cache.popitem()
            self.cache[path] = info
        return info

mdb = pymongo.Connection("localhost").dfs
gfs = gridfs.GridFS(mdb, collection="fs")

try:
    bn = os.path.basename("/")
    dn = os.path.dirname("/")
    gfs.put('', filename=bn, type='dir', dir=dn, uid=0, gid=0, mode=0755)
except:
    pass

memfs = DerpFS(mdb, "fs")
mp = fuse.mount(memfs, "./tmp", foreground=True, fsname="derp-fs")