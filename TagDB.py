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


#
# print traceback
# import traceback
# traceback.print_tb()

class TagDB:
    
    def __init__(self, dbfile):
        """Load tag db from a file"""
        self.tags = {}
        self.files = {}
        pass

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
            if tag in in self.tags:
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
        except NoUniqueTagExcaption:
            pass
        if len(frs) == 0:
            frs = None        

        # query as a directory:
        drs = None
        notagex = None
        try:
            drs = self.__query_dir(qtags)
        except NoTagException as e:
            # this must be re-raised!
            notagex = e
        except NoUniqueTagExcaption:
            pass

        if frs == None && drs == None:
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
        
        if target == 'dir' || (path[-1] == '/' && target == 'unsure'):
            return ('dir', self.__query_dir(tset))
        
        elif target == 'file':
            if path[-1] == '/':
                raise Exception('Sys error: query file but a dir path is '
                                + 'given: ' + path)
            else:
                # use / as tag for files without tag
                if len(fset) == 1:
                    fset = ['/'] + fset
       
                frs = self.__query_file(fset)
                if len(frs) == 0:
                    return ('no such file', )
                elif len(frs) == 1:
                    return ('file', frs[0])
                else:
                    return ('files', frs)
            
        elif target == 'unsure':
            return self.__query_both(fset)
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
        Check if a file with fname as name and 'tags' as all its tags a 
        unique in the database. 
        """
        fpath = ''
        for tag in tags:
            fpath += tag
            if fpath[-1] != '/':
                fpath += '/'
        fpath += fname
        return self.check_unique_filepath(fpath)

    def getftags(self, fuuid):
        """Get tag set by file UUID"""
        pass

    def getsubtags(self, ptags):
        """Get tags that also associated with files queried by 'ptags'"""
        pass

    def getfiles(self, tags):
        """Do query with 'tags' and return the file set"""
        pass

    def addftags(self, fuuid, tags):
        """Attach tags to file"""
        pass

    def addtags(self, tags):
        """Add tags only"""
        pass

    def rmftags(self, fuuid, tags):
        """Remove tags from a file"""
        pass

    def addfile(self, fuuid, fname):
        """Add a new file to db with fuuid and fname as name"""
        pass

    def rmfile(self, fuuid):
        """Remove a file from db"""
        pass

    def load_db(self, dbfile):
        pass


        
