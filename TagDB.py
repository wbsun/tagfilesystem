# Tag db

import stat

class DBFile:
    def __init__(self, fuuid, fname):
        self.fuuid = fuuid
        self.fname = fname
        self.tags = set()
        self.mode = stat.S_IFREG

    def getfullname(self):
        return self.fuuid + '_' + self.fname

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


# for error logger:
# print traceback
# import traceback
# traceback.print_tb()

class TagDB:
    
    def __init__(self, dbfile = None):
        """Load tag db from a file"""
        self.tags = {} # tags is {tag=>{fuuid=>DBFile}}
        self.files = {} # files is {fuuid=>DBFile}
        if dbfile != None:
            self.load_db(dbfile)

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
            else:
                fnames[f.fname] = [fid]
                rs.append((fid,))

        for fn in dupnames:
            fids = fnames[fn]
            for fid in fids:
                unqtags = self.files[fid].tags.copy()
                for other_fid in fids:
                    if fid != other_fid:
                        unqtags -= self.files[other_fid].tags
                if len(unqtags) == 0:
                    raise NoUniqueTagException('Can not distinguish files: ' \
                                               + str(fids), fids)
                rs.append((fid, unqtags.pop()))  

        return rs        
        
    def __query_by_tags(self, qtags):
        fset = None
        for tag in qtags:
            if tag in self.tags:
                raise NoTagException('Can not find tags ' + tag, tag)
            if fset == None:
                fset = set(self.tags[tag].keys())
            else:
                fset &= set(self.tags[tag].keys())
            if len(fset) == 0:
                return []
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
        except NoTagException:
            pass
        except NoUniqueTagException:
            pass
        if len(frs) == 0:
            frs = None        

        # query as a directory:
        drs = None
        notagex = None
        try:
            ds = self.__query_dir(qtags)
        except NoTagException as e:
            # this must be re-raised!
            notagex = e
        except NoUniqueTagException:
            pass

        if frs == None and drs == None:
            raise notagex
        elif drs == None:
            if len(frs) > 1:
                # This make it possible that a command may
                # want to get multiple files with some tags.
                return ('files', frs)
            else:
                return ('file', frs[0])
        elif frs == None:
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
            tset = tset[1:]
        if tset[-1] == '':
            tset = tset[0:-1]
        
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
                    return ('no such file', )
                elif len(frs) == 1:
                    return ('file', frs[0])
                else:
                    return ('files', frs)
            
        elif target == 'unsure':
            return self.__query_both(tset)
        else:
            raise Exception('Invalid parameter: target = %s'%target)

    def check_unique_filepath(self, filepath):
        """
        Check if a file path is unique. The filepath includes a filename
        and directory struture.
        """
        try:
            self.find_by_path(filepath, 'unsure')
        except NoTagException:
            return True
        except NoUniqueTagException:
            return False
        except NameConflictionException:
            return False

        return False

    def check_unique_file(self, tags, fname):
        """
        Check if a file with fname as name and 'tags' as all its tags  """
        fpath = ''
        for tag in tags:
            fpath += tag
            if fpath[-1] != '/':
                fpath += '/'
        fpath += fname
        return self.check_unique_filepath(fpath)
    
    def add_file(self, fuuid, fname, ftags):
        if self.check_unique_file(ftags, fname):
            f = DBFile()
            f.fuuid = fuuid
            f.fname = fname
            f.tags = ftags[:]
                        
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
                                       +ftags+' is not unique.')


    def rmfile(self, fuuid):
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
    
    def store_db(self, dbfile):
        import pickle
        dbf = open(dbfile, 'wb')
        pickle.dump(self.files, dbf)
        pickle.dump(self.tags, dbf)
        dbf.close()



        
