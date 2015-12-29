Design and implement a tag-based file system using Fuse-Python.
This file system uses tags rather than hierarchy directory tree structure to organize files(object may be better). To be compatible with traditional file system in both API and user interfaces, directory names in a file path are treated to be tags of a file. So `ls -l /homework/os/user_space_thread_library/source_file/` will do a tag query like: `select all from files where tags have "homework" AND "os" AND "user_space_thread_library" AND "source_file"`.

The motivation of tagfilesystem is a tag-based Internet in which the URL is tag query. Instead of DNS server, there are tag servers to assist tag-based query. Unlike semantic Web, tags are associated to pages(objects) manually when creating or modifying. It is hard to replace existing Internet architecture. And it is not like a research-oriented project. But I will go on this idea.

# NEWS #

I am considering adding version information to the meta-data file in order to deal with future meta-data changes. -- by Weibin @ 4/20/2010

The poster of TagFS is attached in the downloads. -- by Weibin @ 3/2/2010

I committed a new version in source tree. This version will give a better directory attributes. Also thinking about add timestamps to tags so that the directory a/m/c timestamps will be meaningful. The timestamps of a directory can be the most recent ones of the tags in the path.  -- by Weibin @ 2/4/2010

GUI file browser crash may due to the file name returned when multiple files have the same name in one directory. I will deal with that as the next step. -- by Weibin @ 1/26/2010

tagfilesystem-1.2 is released. readdir will return subtags. So 'ls' will list sub directories as well. Subtags are tags that are also associated with files that are in current directory(or may say current tag query). GUI file browser such as Nautilus on Linux will crash when there are multiple files with the same name in current directory. Consoles deal with this situation very good. GUI browser sucks. -- by Weibin @ 1/24/2010

I am planning to change the behavior of 'readdir'. It will return 'sub tags' too. 'Sub tags' are identity to the result of lstags. But users should keep in mind that there is not hierarchy structure of tags. -- by Weibin @ 1/23/2010

Bug fix - Version 1.1 is released. -- by Weibin @ 1/10/2010

Version 1.0 is released. -- by Weibin @ 1/6/2010

Skeleton code is done. -- by Weibin

# Thanks #
Thanks to Robert Ricci for his kind help. I learnt a lot from our everyweek talk.

# Misc #
I am using tagfilesystem to organize my papers. Try it if you are also a researcher. It really helps. -- by Weibin @ 1/23/2010