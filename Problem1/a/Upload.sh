#!/bin/bash
LOGIN = "adussing"

# Upload files in current directory 
scp -P Wikipedia-En-41784-Articles.tar.gz  $LOGIN@access-iris.uni.lu:~/
scp -P *.sh $LOGIN@access-iris.uni.lu:~/
scp -P *.java $LOGIN@access-iris.uni.lu:~/problem1



