#!/bin/sh

# inside the mount point:
mkdir a
mkdir b
mkdir c
mkdir d
touch a/b/1.t
touch b/c/1.t
touch d/1.t
touch d/b/2.t

ls b > /dev/null
ls d > /dev/null

mv b/2.t a/c/3.t
cp c/3.t a/4.t

rm a/4.t
ls a 
