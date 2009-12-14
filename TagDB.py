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

    def __query_by_tags(self, tags):
        """ do file query by tags  """
        tset = tags.split('/')
        if tset[0] == '':
            tset = tset[1:]
        if len(tset) < 1:
            return []
        
        wantfile = True
        if tset[-1] == '':
            wantfile = False
            tset = tset[0:-1]
        
        fset = None
        for tag in tset:
            if tag not in self.tags:
                raise NoTagException('Can not find tags '+tag, tag)
            if fset == None:
                fset = set(self.tags[tag].keys())
            else:
                fset &= set(self.tags[tag].keys())
            if len(fset) == 0:
                return []
            
        return list(fset)

    def __query_by_tags_unique_list(self, tags):
        flist = self.__query_by_tags(tags)
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


    def __is_unique(self, tags, fname):
        """ Check if a file with fname as name and 'tags' as all its tags a 
        uniquein the database. 
        """
        try:
            flist = self.__query_by_tags(tags)
        except NoTagException:
            return True
        if len(flist) == 0:
            return True

        tset = tags.split('/')
        if tset[0] == '':
            tset = tset[1:]

        if len(tset) > 0:
            if tset[-1] == '':
                tset = tset[0:-1]
        
        tset.append(fname)
        for fid in flist:
            f = self.files[fid]
            if fname == f.fname:
                if len(tset) >= len(f.tags):
                    return False

        return True       

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


        
