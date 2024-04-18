#!/bin/bash
LOGIN="adussing"

# Upload files in current directory 
scp -P 8022 Wikipedia-En-41784-Articles.tar.gz  $LOGIN@access-iris.uni.lu:~/
scp -P 8022 *.sh $LOGIN@access-iris.uni.lu:~/
scp -P 8022 *.java $LOGIN@access-iris.uni.lu:~/problem1



