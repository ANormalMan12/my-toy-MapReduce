# MapReduce implemented in Java

## 运行方法

* Java版本：Java 8
* 开发使用的IDEA版本：2023.3.4(Ultimate)

从Windows端调试：运行`src/main/cmd/copyToMachines.bat`进行快速调试。
调试前应修改该批处理文件中的主机名称，并去除最后数行的注释。

在Linux中运行：将IDEA打包的`out/artifacts/wwyMapReduceOutput`文件夹分别复制到主机和从机的相同绝对路径下。然后在主机运行：
`bash start-mapreduce.sh`
来启动MapReduce服务。可以通过检查同目录下log文件的内容，来确认JobTracker的启动情况。同时，也可以去从机目录下查看TaskTracker的运行情况。此外，还可以使用jps来查看启动情况。 
确认MapReduce服务上线后，可以执行`bash run-client.bash`来向jobTracker提交服务。
## 实现方法

基于Java RMI远程调用库。

## 架构图
![Framework.png](docs%2Fdesigns%2FFramework.png)