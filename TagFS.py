#! /usr/bin/python

# TagFS class in tagfs.
# Mostly derived from xmp.py of python-fuse samples.
# We bring tag-based path query.

import fuse
from fuse import Fuse
from time import time
import stat
import os
import errno
import sys

import TagDB
import tagfsutils


# many projects based on fuse-python use tricks for importing and version,
# we may need that, too.

fuse.fuse_python_api = (0, 2)

import logging
LOG_FILENAME = '/tmp/tagfs.log'
logging.basicConfig(level = logging.DEBUG, \
                    format = '%(asctime)s %(levelname)s %(message)s', \
                    filename = LOG_FILENAME, \
                    filemode = 'w') # or 'a+'

def _flags2mode(flags):
    md = {os.O_RDONLY: 'r', \
          os.O_WRONLY: 'w', \
          os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY|os.O_WRONLY|os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m

class TagfsStat(fuse.Stat):
    def __init__(self):
        pass


class TagFS(fuse.Fuse):
    
    cur_tagfs = None
    
    def __init__(self, *args, **kw):
        global g_cur_tagfs
        Fuse.__init__(self, *args, **kw)
        self.tdb = TagDB.TagDB()
        #tdb.loaddb(None)
        self.lldir = "." # TODO: set low level directory
        TagFS.cur_tagfs = self
        self.root = "."
        
    def getattr(self, path):
        st = TagfsStat()
        try:
            fs = self.tdb.find_by_path(path, 'unsure')
            if fs[0] == 'no such file' or fs[0] == 'files':
                logging.error('getattr: no ent '+path)
                return -errno.ENOENT
        except TagDB.NoTagException:
            logging.error('getattr: no ent '+path)
            return -errno.ENOENT

        st.st_size = 4096L
        st.st_nlinks = 2
        st.st_ino = 0L
        st.st_dev = 0L
        st.st_gid = 0
        st.st_atime = 0
        st.st_mtime = 0
        st.st_ctime = 0
            
        if fs[0] == 'dir':
            # directory
            st.st_mode = stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR \
                       | stat.S_IRGRP | stat.S_IWGRP
            
        else:
            # file
            llst = os.lstat(self.lldir + self.tdb.files[fs[1][0]].getfullname())
            st.st_size = llst.st_size
            st.st_nlinks = llst.st_nlinks
            st.st_ino = llst.st_ino
            st.st_dev = llst.st_dev
            st.st_gid = llst.st_gid
            st.st_atime = llst.st_atime
            st.st_mtime = llst.st_mtime
            st.st_ctime = llst.st_ctime
            st.st_mode = llst.st_mode
        return st

    def readlink(self, path):
        # link is not supported
        return -errno.ENOSYS

    def readdir(self, path, offset):
        try:
            fs = self.tdb.find_by_path(path, 'dir')
            if fs[0] == 'file':
                yield -errno.ENOTDIR
                return
            if fs[0] != 'dir':
                yield -errno.ENOENT
                return
        except TagDB.NoTagException:
            yield -errno.ENOENT
            return
        
        for f in fs[1]:
            if len(f) == 1:
                yield fuse.Direntry(self.tdb.files[f[0]].fname)
            else:
                yield fuse.Direntry(f[1] + '/' + self.tdb.files[f[0]].fname)

    def unlink(self, path):
        try:
            fs = self.tdb.find_by_path(path, 'file')
            if fs[0] != 'file':
                return -errno.ENOENT
            # (TODO: add rm_file_tags_by_path(path, uuid) to TagDB)
            self.tdb.rm_file_tags_by_path(path, fs[1][0])
            self.tdb.store_db(self.lldir+'.tagfs_db.meta')
        except TagDB.NoTagException:
            return -errno.ENOENT
        except TagDB.NoUniqueTagException:
            return -errno.EISDIR # -errno.ENOENT may be better
        
        # unlink will remove the tags associated with the file, if there is
        # not any tag left, remove the file, too.
        
    def rmdir(self, path):
        try:
            fs = self.tdb.find_by_path(path, 'dir')
            if len(fs[1]) != 0:
                return -errno.ENOTEMPTY # not empty
            else:
                self.tdb.rm_tags_by_path(path)
                self.tdb.store_db(self.lldir+'.tagfs_db.meta')
        except TagDB.NoTagException:
            return -errno.ENOENT
        except TagDB.NoUniqueTagException:
            logging.error('Can not remove dir because of name confliction')
            return -errno.EFAULT # but there is problem! (TODO: figure out a solution)

    def symlink(self, path, path1):
        # I decide not support symlink!
        return -errno.ENOSYS

    def rename(self, path, path1):
        # not support yet
        # self.tdb.store_db(self.lldir+'.tagfs_db.meta')
        return -errno.ENOSYS

    def link(self, path, path1):
        # I decide not support link!
        return -errno.ENOSYS

    def chmod(self, path, mode):
        try:
            frs = self.tdb.find_by_path(path, 'unsure')
            if frs[0] == 'files':
                return -errno.ENOENT
            elif frs[0] == 'dir':
                logging.info('chmod on dir makes no sense')
                return 0
            elif frs[0] == 'file':
                os.chmod(self.lldir+self.tdb.files[frs[1][0]].getfullname(), mode)
        except:
            return -errno.ENOENT

    def chown(self, path, user, group):
        try:
            frs = self.tdb.find_by_path(path, 'unsure')
            if frs[0] == 'files':
                return -errno.ENOENT
            elif frs[0] == 'dir':
                logging.info('chown on dir makes no sense')
                return 0
            elif frs[0] == 'file':
                os.chown(self.lldir+self.tdb.files[frs[1][0]].getfullname(), user, group)                
        except:
            return -errno.ENOENT

    def truncate(self, path, len):
        try:
            frs = self.tdb.find_by_path(path, 'unsure')
            if frs[0] == 'files':
                return -errno.ENOENT
            elif frs[0] == 'dir':
                logging.info('truncate on dir')
                return -errno.EISDIR
            elif frs[0] == 'file':
                f = open(self.lldir+self.tdb.files[frs[1][0]].getfullname(), 'a')
                f.truncate(len)
                f.close()                
        except:
            return -errno.ENOENT

    def mknod(self, path, mode, dev):
        logging.info('Create new inode')
        ftags_rs = tagfsutils.path2tags(path, 'file')
        if ftags_rs[0] == 'file':
            logging.info('Want to create a file')
            ftags = ftags_rs[1][0:-1]
            fname = ftags_rs[1][-1]
            import uuid
            fuuid = uuid.uuid4().hex
            try:
                self.tdb.add_file(fuuid, fname, ftags)
            except:
                logging.error('Want create a file that conflicts with tags')
                e = OSError()
                e.errno = errno.EEXIST # (TODO: find a correct errno)
                raise e
        else:
            logging.info('Want to create a dir')
            self.mkdir(path, mode)
        self.tdb.store_db(self.lldir+'.tagfs_db.meta')


    def mkdir(self, path, mode):
        self.tdb.add_tags_by_path(path)
        self.tdb.store_db(self.lldir+'.tagfs_db.meta')

    def utime(self, path, times):
        try:
            frs = self.tdb.find_by_path(path, 'unsure')
            if frs[0] == 'files':
                return -errno.ENOENT
            elif frs[0] == 'dir':
                logging.info('utime on dir makes no sense')
                return 0
            elif frs[0] == 'file':
                return os.utime(self.lldir+self.tdb.files[frs[1][0]].getfullname()
                                 ,times)
        except:
            return -errno.ENOENT

    def access(self, path, mode):
        try:
            frs = self.tdb.find_by_path(path, 'unsure')
            if frs[0] == 'files':
                return -errno.ENOENT
            elif frs[0] == 'dir':
                return 0
            elif frs[0] == 'file':
                if not os.access(self.lldir+self.tdb.files[frs[1][0]].getfullname()
                                 ,mode):
                    return -errno.EACCES
        except:
            return -errno.ENOENT
        
    def fsinit(self):
        self.lldir = self.root
        if self.lldir[-1] != '/':
            self.lldir += '/'
        if os.path.exists(self.lldir+'.tagfs_db.meta'):
            self.tdb.load_db(self.lldir+'.tagfs_db.meta')
        os.chdir(self.root)
        print self.root

    class TagFSFile(object):

        def __init__(self, path, flags, *mode):
            self.tagfs = TagFS.cur_tagfs  
            self.path = path
            self.flags = flags
            self.mode = mode
            try:
                # can a directory be opened? Yes, but when reading, errors are there.
                f = self.tagfs.tdb.find_by_path(path, 'unsure')
                if f[0] == 'files':
                    e = OSError()
                    e.errno = errno.ENOENT
                    raise e
                elif f[0] == 'dir':
                    self.filetype = 'dir'                    
                    self.dircont = f[1]
                else:
                    self.filetype = 'file'
                    self.file = os.fdopen(os.open(
                                   self.tagfs.lldir + self.tagfs.tdb.files[f[1][0]].getfullname(),
                                   flags, *mode), _flags2mode(flags))
                    self.fd = self.file.fileno()
            except (TagDB.NoTagException, TagDB.NameConflictionException):
                e = OSError()
                e.errno = errno.ENOENT
                raise e
            except TagDB.NoUniqueTagException:
                # System error: no unique id for path/file
                logging.error('System error: no unique id for path/file: path='+path)
                e = OSError()
                e.errno = errno.ENOENT
                raise e
            except TagDB.NoFileException:
                logging.info('Create new file')
                if flags | os.O_CREAT == flags:
                    ftags_rs = tagfsutils.path2tags(path, 'file')
                    if ftags_rs[0] != 'file':
                        logging.error('Want to create a directory')
                        e = OSError()
                        e.errno = errno.EISDIR # (TODO: check the correct errno)
                        raise e
                    ftags = ftags_rs[1][0:-1]
                    fname = ftags_rs[1][-1]
                    import uuid
                    fuuid = uuid.uuid4().hex
                    try:
                        self.tagfs.tdb.add_file(fuuid, fname, ftags)
                        self.file = os.fdopen(os.open(
                                   self.tagfs.lldir + self.tagfs.tdb.files[fuuid].getfullname(),
                                   flags, *mode), _flags2mode(flags))
                        self.fd = self.file.fileno()                        
                    except:
                        logging.error('Want create a file that conflicts with tags')
                        e = OSError()
                        e.errno = errno.EEXIST # (TODO: find a correct errno)
                        raise e
                else:
                    e = OSError()
                    e.errno = errno.ENOENT
                    raise e
            
        def __fail_dir_ops(self):
            if self.filetype == 'dir':
                e = OSError()
                e.errno = errno.ENOSYS
                raise e

        def read(self, length, offset):
            self.__fail_dir_ops()                
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):
            self.__fail_dir_ops()   
            self.file.seek(offset)
            self.file.write(buf)
            return len(buf)

        def release(self, flags):
            self.file.close()
            if self.flags | os.O_CREAT == self.flags:
                self.tagfs.tdb.store_db(self.tagfs.lldir+'.tagfs_db.meta')

        def _fflush(self):
            self.__fail_dir_ops()   
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            self.__fail_dir_ops()   
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            self.__fail_dir_ops()   
            self._fflush()
            os.close(os.dup(self.fd))
            if self.flags | os.O_CREAT == self.flags:
                self.tagfs.tdb.store_db(self.tagfs.lldir+'.tagfs_db.meta')

        def fgetattr(self):
            if self.filetype == 'dir':
                return self.cur_tagfs.getattr(self.path) 
            return os.fstat(self.fd)

        def ftruncate(self, len):
            self.__fail_dir_ops()   
            self.file.truncate(len)
          
    def main(self, *a, **kw):

        self.file_class = self.TagFSFile
        self.fsinit()

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
            print server.root
    except OSError:
        print >> sys.stderr, "can't enter root of underlying filesystem"
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    main()

