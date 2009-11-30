# Tag db

class DBFile:
    def __init__(self, fuuid, fname):
        self.fuuid = fuuid
        self.fname = fname
        self.tags = []

    def getfullname(self):
        return self.fuuid + '_' + self.fname


class TagDB:
    def __init__(self, dbfile):
        """Load tag db from a file"""
        pass

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


        
