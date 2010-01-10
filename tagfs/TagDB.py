# Tag db

import stat
import tagfsutils

class DBFile:
    def __init__(self, fuuid, fname, ftags):
        self.fuuid = fuuid
        self.fname = fname
        self.tags = ftags
        self.mode = stat.S_IFREG|0777

    def getfullname(self):
        return self.fuuid + '_' + self.fname
    
    def __str__(self):
        return 'DBFile(fuuid='+self.fuuid+', fname='+ \
                self.fname+', tags='+str(self.tags)+')'
    
    def __repr__(self):
        return self.__str__()

class NoTagException(Exception):
    def __init__(self, msg, tags):
        self.msg = msg
        self.tags = tags

class NoUniqueTagException(Exception):
    def __init__(self, msg, files):
        self.msg = msg
        self.files = files

class NameConflictionException(Exception):
    def __init__(self, msg):
        self.msg = msg
        
class NoFileException(Exception):
    def __init__(self, msg, file):
        self.msg = msg
        self.file = file

# for default tags db file
DefaultMetaDBFile = '.tagfs_db.meta'

class TagDB:
    
    def __init__(self, logger, dbfile = None):
        """Load tag db from a file"""
        self.tags = {'/':{}} # tags is {tag=>{fuuid=>DBFile}}
        self.files = {} # files is {fuuid=>DBFile}
        if dbfile != None:
            self.load_db(dbfile)
        self.logger = logger

    def __make_unique(self, flist):
        """
        make a file list unique in names
        """
        rs = []
        fnames = {}
        dupnames = []
        for fid in flist:
            f = self.files[fid]
            if f.fname in fnames:
                fnames[f.fname].append(fid)
                if len(fnames[f.fname]) == 2:
                    dupnames.append(f.fname)
                    rs.remove((fnames[f.fname][0],))
            else:
                fnames[f.fname] = [fid]
                rs.append((fid,))

        for fn in dupnames:
            fids = fnames[fn]
            
            # About notagused:
            #
            # If there are multiple files, say n, with the same name in the 
            # current query, ideally we need n different tags to distinguish 
            # them, in a way that each file has its unique tag attached in its
            # file name. However, to distinguish them, only n-1 different tags
            # are enough because one of those files can use its original name
            # without any unique tag.
            # This is cool when ls, but not that good when a user wants to open
            # that file without unique tag because the query will result in
            # a 'files' result. 
            # So the file query must be aware of this.
            # 
            #                                                  -by Weibin Sun
            
            # enable notagused feature:
            notagused = False
            for fid in fids:
                unqtags = set(self.files[fid].tags[:])
                for other_fid in fids:
                    if fid != other_fid:
                        unqtags -= set(self.files[other_fid].tags)
                if len(unqtags) == 0:
                    
                    # enable notagused feature:
                    #
                    if notagused:
                        raise NoUniqueTagException('Can not distinguish files: ' \
                                               + str(fids), fids)
                    else:
                        rs.append((fid, ))
                        notagused = True
                    
                    # raise NoUniqueTagException('Can not distinguish files: ' \
                    #                           + str(fids), fids)
                else:
                    rs.append((fid, unqtags.pop()))  

        return rs        
        
    def __query_by_tags(self, qtags):
        fset = None
        for tag in qtags:
            if tag not in self.tags:
                self.logger.error('query by tags no tag: '+tag)
                raise NoTagException('Can not find tags ' + tag, tag)
            if fset == None:
                fset = set(self.tags[tag].keys())
            else:
                fset &= set(self.tags[tag].keys())
        if fset == None:
            self.logger.error('query by tags no tag: '+tag)
            raise NoTagException('Can not find tags ' + tag, tag)
        return list(fset)

    def __query_file(self, qtags):
        """
        qtags: tags splited from path, the last one is filename
        """        
        flist = self.__query_by_tags(qtags[0:-1])
        flist = self.__make_unique(flist)
        rs = []
        for f in flist:
            if self.files[f[0]].fname == qtags[-1]:
                rs.append(f)
        return rs

    def __query_dir(self, qtags):
        flist = self.__query_by_tags(qtags)
        self.logger.info('query dir: '+str(qtags)+str(flist))
        flist = self.__make_unique(flist)
        return flist

    def __query_both(self, qtags):                
        # tset[-1] != '' because that would be query for dir.
        ftags = qtags
        frs = None
        if len(ftags) == 1:
            ftags = ['/'] + ftags
        try:
            frs = self.__query_file(ftags)
            if len(frs) == 0:
                frs = [] 
        except NoTagException:
            self.logger.info('query both file part no tag: '+str(ftags))
        except NoUniqueTagException:
            self.logger.info('query both file part no unique tag: '+str(ftags))
        
        self.logger.info('query both file part: '+str(frs)+' for '+str(ftags))
              
        # query as a directory:
        drs = None
        notagex = None
        try:
            drs = self.__query_dir(qtags)
        except NoTagException as e:
            # this must be re-raised!
            self.logger.info('query both dir part no tag: '+str(qtags))
            notagex = e
        except NoUniqueTagException as e:
            self.logger.info('query both dir part no unique tag: '+str(qtags) + ' '+e.msg)
            

        self.logger.info('query both dir part: '+str(drs)+' for '+str(qtags))
        
        if frs == None and drs == None:            
            raise notagex
        elif drs == None:
            if len(frs) > 1:
                # This make it possible that a command may
                # want to get multiple files with some tags.
                return ('files', frs)
            elif len(frs) == 1:
                return ('file', frs[0])
            else:
                return ('no file',) 
        elif frs == None or frs == []:
            return ('dir', drs)
        else:
            # this is really a terrible situation!
            raise NameConflictionException('Can\'t distinguish file and dir '\
                                           + 'with tags: ' + str(qtags))

    def find_by_path(self, path, target):
        """
        @description:
          Do query by tags in path.
        @param:
          @path: file path consists of tags and maybe a filename
          @target: the target the upper level wants.
                   can be 'file', 'dir' and 'unsure'.
        @return:
          Results are different according to different cases:
          ... (TODO: add descriptions of cases)
        """
        if len(path) == 0:
            raise NoTagException('No such file.')

        tset = path.split('/')
        if tset[0] == '':
            del tset[0]
        if tset[-1] == '':
            del tset[-1]
        if len(tset) == 0:
            tset = ['/']
        
        if target == 'dir' or (path[-1] == '/' and target == 'unsure'):
            return ('dir', self.__query_dir(tset))
        
        elif target == 'file':
            if path[-1] == '/':
                raise Exception('Sys error: query file but a dir path is '
                                + 'given: ' + path)
            else:
                # use / as tag for files without tag
                if len(tset) == 1:
                    tset = ['/'] + tset
       
                frs = self.__query_file(tset)
                if len(frs) == 0:
                    raise NoFileException('No such file', path)
                elif len(frs) == 1:
                    return ('file', frs[0])
                else:
                    return ('files', frs)
            
        elif target == 'unsure':
            rs = self.__query_both(tset)
            self.logger.info('query both: '+str(rs)+'for '+path)
            if rs[0] == 'no file':
                raise NoFileException('No such file', path)
            return rs
        else:
            raise Exception('Invalid parameter: target = '+target)

    def check_unique_filepath(self, filepath, existed=False):
        """
        Check if a file path is unique. The filepath includes a filename
        and directory structure.
        """
        try:
            rs = self.find_by_path(filepath, 'unsure')
            self.logger.info('check unique: '+str(rs)+' '+filepath)
            if (rs[0] == 'files' or rs[0] == 'file') and existed:
                return True
            
            if not existed:
                if rs[0] == 'files':
                    for f in rs[1]:
                        if len(f) == 1:
                            return False
                    return True
                if rs[0] == 'file':
                    rspath = '/'
                    for t in self.files[rs[1][0]].tags:
                        rspath += t+'/'
                    rspath += self.files[rs[1][0]].fname
                    if len(rspath) > len(filepath):
                        return True
            return False
        except NoTagException:
            return True
        except NoUniqueTagException:
            return False
        except NameConflictionException:
            return False
        except NoFileException:
            return True

    def check_unique_file(self, tags, fname, existed=False):
        """
        Check if a file with fname as name and 'tags' as all its tags  """
        fpath = '/'.join(['']+[t for t in tags if t != '/']+[fname])        
        return self.check_unique_filepath(fpath, existed)
    
    def add_file(self, fuuid, fname, ftags):
        if self.check_unique_file(ftags, fname):
            newftags = ftags[:]
            if '/' in newftags:
                newftags = newftags[1:]
            f = DBFile(fuuid, fname, newftags)                        
            self.files[fuuid] = f
            if len(ftags) == 0:
                ftags += ['/']
            
            for t in ftags:
                if t in self.tags:
                    self.tags[t][fuuid] = f
                else:
                    self.tags[t] = {fuuid:f}
        else:
            raise NoUniqueTagException('File with name '+fname+' and tags: '\
                                       +str(ftags)+' is not unique.')

    def add_file_tags(self, fuuid, ftags):
        f = self.files[fuuid]
        if self.check_unique_file(ftags+f.tags, f.fname):
            for t in ftags:
                if t != '/' and t not in f.tags:
                    f.tags.append(t)
                if t in self.tags:
                    self.tags[t][fuuid] = f
                else:
                    self.tags[t] = {fuuid:f}
        else:
            raise NoUniqueTagException('File '+fuuid+' can not have tags: ' \
                                       + str(ftags) + ', not unique.')
            
        
    def rm_file(self, fuuid):
        """Remove a file from db"""
        if fuuid in self.files: 
            f = self.files[fuuid]
            del self.files[fuuid]
            for t in f.tags:
                del self.tags[t][fuuid]
            if fuuid in self.tags['/']:
                del self.tags['/'][fuuid]
        else:
            raise Exception('No such file: '+fuuid)

    def load_db(self, dbfile):
        """load metadata from dbfile"""
        import pickle
        dbf = open(dbfile, 'rb')
        self.files = pickle.load(dbf)
        self.tags = pickle.load(dbf)
        dbf.close()
        self.logger.info('DB loaded:')
        self.logger.info('files: '+str(self.files))
        self.logger.info('tags: '+str(self.tags))
    
    def store_db(self, dbfile):
        import pickle
        dbf = open(dbfile, 'wb')
        pickle.dump(self.files, dbf)
        pickle.dump(self.tags, dbf)
        dbf.close()

    def __rm_ftags(self, fuuid, ftags):
        f = self.files[fuuid]
        for t in ftags:
            if t != '/' and t in f.tags:
                f.tags.remove(t)
            del self.tags[t][fuuid]
    
    def __undo_rm_ftags(self, fuuid, ftags):
        f = self.files[fuuid]
        for t in ftags:
            if t != '/' and t not in f.tags:
                f.tags.append(t)
            self.tags[t][fuuid] = f
        
    def rm_file_tags_by_path(self, fuuid, path):
        tset = tagfsutils.path2tags(path, 'file')[1]
        del tset[-1]                
        self.__rm_ftags(fuuid, tset)
        f = self.files[fuuid]
        if not self.check_unique_file(f.tags, f.fname, True):
            self.logger.error('not unique in rm file tags by path'+str(f.tags))
            self.__undo_rm_ftags(fuuid, tset)            
            raise NoUniqueTagException(
                    'Can not make file unique if remove tags. ' \
                    + 'file: ' + fuuid + ' tags: ' + str(tset), tset)
        #if path != '/':
            #if len(f.tags) == 0:
                #self.tags['/'][fuuid] = f
        if len(f.tags) == 0 and fuuid not in self.tags['/']:
            del self.files[fuuid]
            return (True, f.getfullname())
        return (False,)
        
    def rm_tags_by_path(self, path):
        tset = tagfsutils.path2tags(path, 'dir')[1]
        for t in tset:
            if len(self.tags[t]) == 0:
                del self.tags[t]
                
    def change_file_tags(self, fuuid, rmtags, addtags):
        self.logger.info('change_file_tags: +'+str(addtags)+' -'+str(rmtags))
        self.__rm_ftags(fuuid, rmtags)
        try:
            self.add_file_tags(fuuid, addtags)
        except NoUniqueTagException:
            self.__undo_rm_ftags(fuuid, rmtags)
            self.logger.error('change file tags failed rm: '+str(rmtags)+' add: '+str(addtags))
            raise NoUniqueTagException('change file tags failed rm: '+str(rmtags)+' add: '+str(addtags))
    
    def add_tags_by_path(self, path):
        tset = tagfsutils.path2tags(path, 'dir')[1]
        self.logger.info('add tags by path: '+str(tset))
        # TODO: check unique for tags!!! IMPORTANT TODO
        for t in tset:
            if t not in self.tags:
                self.tags[t] = {}

