#! /usr/bin/python

# TagFS class in tagfs.
# Mostly derived from xmp.py of python-fuse samples.
# We bring tag-based path query.

from fuse import Fuse
from time import time
import stat
import os
import errno
import sys

import TagDB
import DBFile

# many projects based on fuse-python use tricks for importing and version,
# we may need that, too.

fuse.fuse_python_api = (0, 2)

import logging
Log_Filename = '/tmp/tagfs.log'
logging.basicConfig(level = logging.DEBUG, \
                    format = '%(asctime)s %(levelname)s %(message)s', \
                    filename = Log_Filename, \
                    filemode = 'w') # or 'a+'

def _flags2mode(flags):
    md = {os.O_RDONLY: 'r', \
          os.O_WRONLY: 'w', \
          os_ORDWR: 'w+'}
    m = md[flags & (os.O_RDONLY|os.O_WRONLY|os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m

class TagfsState(fuse.Stat):
    def __init__(self):
        pass

class TagFS(fuse.Fuse):
    
    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        tdb = TagDB(None)
        #tdb.loaddb(None)
        lldir = "" # TODO: set low level directory
        
    def getattr(self, path):
        st = TagfsStat()
        try:
            fs = tdb.getfiles(path)
        except NoTagException as e:
            return -errno.ENOENT

        st.st_size = 4096L
        st.st_nlinks = 2
        st.st_ino = 0L
        st.st_dev = 0L
        st.st_gid = 0
        st.st_atime = 0
        st.st_mtime = 0
        st.st_ctime = 0
            
        if isinstance(fs, list):
            # directory
            st.st_mode = stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR \
                       stat.S_IRGRP | stat.S_IWGRP
            
        else:
            # file
            llst = os.lstat(lldir + fs.getfullname())
            st = llst

        return st

    def readlink(self, path):
        raise OSError("readlink is not supported in TagFS")
        #return os.readlink("." + path)

    def readdir(self, path, offset):
        try:
            fs = tdb.getfiles(path)
        except NoTagException as e:
            return -errno.ENOENT

        if isinstance(fs, DBFile):
            return -errno.ENOTDIR
        
        for f in fs:
            if len(f) == 1:
                yield fuse.Direntry(tdb.files[f[0]].fname)
            else:
                yield fuse.Direntry(f[1] + '/' + tdb.files[f[0]].fname)

    def unlink(self, path):
        try:
            fs = tdb.getfiles(path)
        except NoTagException:
            return -errno.ENOENT
        except NoUniqueTagException:
            return -errno.EISDIR

        if isinstance(fs, list):
            return -errno.EISDIR
        # unlink will remove the tags associated with the file, if there is
        # not any tag left, remove the file, too.
        # file name is also a tag, so no-more-tag means len(f.tags) == 1
        #
        
    def rmdir(self, path):
        os.rmdir("." + path)

    def symlink(self, path, path1):
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        os.rename("." + path, "." + path1)

    def link(self, path, path1):
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        os.chmod("." + path, mode)

    def chown(self, path, user, group):
        os.chown("." + path, user, group)

    def truncate(self, path, len):
        f = open("." + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
        os.mkdir("." + path, mode)

    def utime(self, path, times):
        os.utime("." + path, times)

    def access(self, path, mode):
        if not os.access("." + path, mode):
            return -EACCES

    class TagFSFile(object):

        def __init__(self, path, flags, *mode):
            self.file = os.fdopen(os.open("." + path, flags, *mode), \
                                  flag2mode(flags))
            self.fd = self.file.fileno()

        def read(self, length, offset):
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):
            self.file.seek(offset)
            self.file.write(buf)
            return len(buf)

        def release(self, flags):
            self.file.close()

        def _fflush(self):
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            self._fflush()
            os.close(os.dup(self.fd))

        def fgetattr(self):
            return os.fstat(self.fd)

        def ftruncate(self, len):
            self.file.truncate(len)
          
    def main(self, *a, **kw):

        self.file_class = self.TagFSFile

        return Fuse.main(self, *a, **kw)

def main():
    usage = """
Userspace tag based file system.

""" + Fuse.fusage

    server = TagFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.multithreaded = False

    server.parser.add_option(mountopt="root", metavar="PATH", default='~/',
            help="mirror filesystem from under PATH [default: %default]")
    server.parse(values=server, errex=1)

    try:
        if server.fuse_args.mount_expected():
            os.chdir(server.root)
    except OSError:
        print >> sys.stderr, "can't enter root of underlying filesystem"
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    main()

