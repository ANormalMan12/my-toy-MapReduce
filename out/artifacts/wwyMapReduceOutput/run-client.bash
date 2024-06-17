#!/bin/bash

taskClass="ANormalMan12.mymapreduce.tasktracker.TaskTrackerMain"
jobClass="ANormalMan12.mymapreduce.jobtracker.JobTrackerMain"
clientClass="ANormalMan12.mymapreduceApp.WordCount.WordCountMain"

jarPath="/home/wwy/wwyMapReduceProgram/wwyMapReduce.jar"
javaPath="/home/wwy/.jdks/ibm-java-x86_64-80/bin/java"

lsof -ti tcp:12052 | xargs kill ; $javaPath -cp $jarPath $clientClass  "wwy-VirtualBox" "/home/wwy/wwyMapReduceProgram/word_count_test.txt" "/home/wwy/wwyMapReduceProgram/wwyMapReduce.jar"
