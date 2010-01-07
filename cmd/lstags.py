#! /usr/bin/python

"""
lstags wants to help the 'ls' command to list all tags of a
file.

Features include:
    1 list all tags of a file
    2 list all tags also attached to the files under an existing tag-query, i.e. current dir
    3 list all tags when under 'root' of mounted file system
    
Implemented:
    1, 2, 3
    
We use xattr to implement lstags.
"""
import xattr
import sys
import os

def lstags(path):
    tags = xattr.get(path,'tags')
    
    if os.path.isdir(path):
        subtags = tags.split('/')
        for t in subtags:
            print t+'\t',
        print ""
    else:
        print 'tags: '+tags


if __name__ == '__main__':
    path = ''
    if len(sys.argv) == 1:
        path = '.'
    else:
        path = sys.argv[1]
    
    try:
        lstags(os.path.realpath(path))            
        sys.exit(0)
    except IOError as ioe:
        print ioe.strerror
    except Exception as e:
        print str(e)
    
    sys.exit(1)
