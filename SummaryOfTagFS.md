# TagFS - Tag File System #

---

## Related projects and papers: ##
  1. http://www.tagsistant.net/
  1. http://code.google.com/p/xtagfs/
  1. http://code.google.com/p/dhtfs/
  1. http://code.google.com/p/tag-fs/
  1. http://github.com/xlevus/StagFS
  1. TagFS:
    1. http://www.aifb.kit.edu/web/Inproceedings1140
    1. http://www.slideshare.net/xamde/tagfs-tag-semantics-for-hierarchical-file-systems-2183080
  1. Tag file system in D and its survey: http://nascent.freeshell.org/programming/TagFS/
  1. Semantic FS:
    1. Copernicus: A Scalable, High-Performance Semantic File System http://www.ssrc.ucsc.edu/Papers/ssrctr-09-06.pdf
    1. Semantic File Systems (A survey) http://www.objs.com/survey/OFSExt.htm
    1. Semantic File Systems (paper by MIT) http://www.psrg.csail.mit.edu/history/publications/Papers/sfsabs.htm
    1. http://semanticweb.org/wiki/SemFS
    1. The damn patent: http://www.patents.com/Semantic-file-system/US7617250/en-US/
    1. http://www.hipc.org/hipc2006/posters/semfs.pdf
    1. The Sile Model - A Semantic File System Infrastructure for the Desktop http://data.semanticweb.org/conference/eswc/2009/paper/70/html

---

## Requirements: ##
### Description: ###
> Design and implement a tag-based file system using Python-FUSE.
> This file system uses tags rather than hierarchy directory tree structure to organize files(object may be better). To be compatible with traditional file system in both API and user interfaces, directory names in a file path are considered to be tags of a file. So `ls -l /homework/os/user_space_thread_library/source_file/` will do a tag query like: `select * from files where tags have "homework" AND "os" AND "user_space_thread_library" AND "source_file"`.

### Features: ###
  1. Initially an overlay on top of traditional file system APIs.
  1. All files in the same directory from a traditional file system view.
  1. A .tagdb meta-date file as tags database and index for path query.
  1. File name is defined to be: {UUID}`_`{readable-name}
  1. To locate an exact file, there are two methods:
    1. Do a tag query, and the result has only one file and the file name is same to the last tag of the path.
    1. Use a UUID.
  1. Tags and index format:
    1. tags hash table for all files with 'tag' key
    1. files hash table for all tags attached to a file
    1. Double references causes complexity. But OK now.
  1. Query, given a path:
    1. As a UUID, find in files hash table, if there, done, else, continue.
    1. Get the intersection of sets of tags in the path.
    1. Whether a path is file or directory should be decided with the help of TagFS class functions because it knows what the current target is. Filename should not be in the tag set of a file. And put file name in the middle of a path is invalid.
  1. Unlink: remove all tags in the path and if tag set of the file is empty, delete it.
  1. Link: add new tags.
  1. So:
    1. cp means create a copied file with new tags.
    1. mv means remove some tags and add some new tags.
    1. ln means add new tags.
    1. rm means remove some tags or the file if all tags are listed in the path.
  1. File name and the extension may also as tags. (Later)

---

## Design: ##
### Code: ###
  1. TagFS: for file system operations, focus on:
    1. getxattr() add tags as attributes
    1. readdir() query tags
    1. rmdir() remove the tag from all files with it
    1. mkdir() create a new tag, without associated files
    1. symlink/link/unlink() operate on tags
    1. open() query tags and create file
    1. readlink() not supported
    1. rename() updates tags
  1. TagStat`:` file stat, with tag support. (tag list)
  1. Logger: for debug and logging.
  1. TagDB: index of tags, maintain tags->file & file->tags mapping and query.

### Misc: ###
  1. Optimize TagDB, organize index in B+tree or radix tree.
  1. Optimize tags query algorithm to utilize: multi-core?
  1. Learning relations between similar tags like 'photo' and 'picture' then optimize tag database.

### Toolkit: (Later) ###
  1. mkfs.tagfs  script to make TagFS partition, build meta-data database or anything else.
  1. mount.tagfs  script for mounting, check meta-data database, etc.
  1. addtags `[`option`]` file `[`tags`]`
  1. rmtags `[`option`]` `[`file`]` `[`tags`]`
  1. lstags.py command to list tags of a file and 'sub-tags' under a directory

