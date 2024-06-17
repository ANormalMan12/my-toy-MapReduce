
set localProjectPath=C:\Users\wangw\IdeaProjects\wwyMapReduce\out\artifacts\wwyMapReduceOutput\
set remoteProjectPath=/home/wwy/wwyMapReduceProgram/
set content=*
::set content=*

scp -r %localProjectPath%%content% wwy@wwy-VirtualBox:%remoteProjectPath%
scp -r wwy@wwy-VirtualBox:%remoteProjectPath%%content% wwy@wwy-slave-1:%remoteProjectPath%
scp -r wwy@wwy-VirtualBox:%remoteProjectPath%%content% wwy@wwy-slave-2:%remoteProjectPath%
:: ssh wwy@wwy-VirtualBox "nohup rmiregistry &"
:: ssh wwy@wwy-VirtualBox "ssh wwy@wwy-slave-1 ""nohup rmiregistry &"" "
:: ssh wwy@wwy-VirtualBox "ssh wwy@wwy-slave-2 ""nohup rmiregistry &"" "

set taskClass=ANormalMan12.mymapreduce.tasktracker.TaskTrackerMain
set jobClass=ANormalMan12.mymapreduce.jobtracker.JobTrackerMain
set jarPath=/home/wwy/wwyMapReduceProgram/wwyMapReduce.jar
set javaPath=/home/wwy/.jdks/ibm-java-x86_64-80/bin/java

::start cmd /k ssh wwy@wwy-VirtualBox "lsof -ti tcp:12050 | xargs kill  ; %javaPath% -cp %jarPath% %jobClass%"
::start cmd /k ssh  wwy@wwy-VirtualBox "echo Connection to VMhost ; ssh wwy@wwy-slave-1  ""echo here is slave-1 && lsof -ti tcp:12051 | xargs kill ; %javaPath% -cp %jarPath% %taskClass% wwy-slave-1 "" "
::start cmd /k ssh  wwy@wwy-VirtualBox "echo Connection to VMhost ; ssh wwy@wwy-slave-2  ""echo here is slave-2 && lsof -ti tcp:12051 | xargs kill ; %javaPath% -cp %jarPath% %taskClass% wwy-slave-2 "" "
