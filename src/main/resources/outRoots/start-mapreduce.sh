#!/bin/bash

taskClass=ANormalMan12.mymapreduce.tasktracker.TaskTrackerMain
jobClass=ANormalMan12.mymapreduce.jobtracker.JobTrackerMain
jarPath=/home/wwy/wwyMapReduceProgram/wwyMapReduce.jar
javaPath=/home/wwy/.jdks/ibm-java-x86_64-80/bin/java

# 连接到虚拟机并启动JobTracker
lsof -ti tcp:12050 | xargs kill; nohup $javaPath -cp $jarPath $jobClass > jobTracker.log 2>&1 &
slave1=wwy-slave-1
slave2=wwy-slave-2

# 连接到slave-1并启动TaskTracker
echo "Connection to VMhost"
ssh wwy@$slave1 "echo here is slave-1 && lsof -ti tcp:12051 | xargs kill; nohup $javaPath -cp $jarPath $taskClass $slave1 > tasktracker_slave1.log 2>&1 &"

# 连接到slave-2并启动TaskTracker
echo "Connection to VMhost"
ssh wwy@$slave2 "echo here is slave-2 && lsof -ti tcp:12051 | xargs kill;nohup $javaPath -cp $jarPath $taskClass $slave2 > tasktracker_slave2.log 2>&1 &"
