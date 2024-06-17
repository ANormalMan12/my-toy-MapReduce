package ANormalMan12.mymapreduce.tasktracker;

import ANormalMan12.mymapreduce.program.worker.WorkerExeClass;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.UUID;

public class Worker {

    public static void main(String[] args){
        //TaskTracker通过检测进程的死活来管理Worker
        String localPreLoadedJarPath=args[0];//加载类的内容
        String className=args[1];//获得类名，也就是Mapper,Reducer的实现类的路径
        int indexNumber=Integer.valueOf(args[2]);//获取indexNumber，也就是待处理的分片
        UUID jobId= UUID.fromString(args[3]);//获取当前task所属的jobId
        UUID taskTrackerID=UUID.fromString(args[4]);//获取当前task所属的taskTrackerId
        System.out.println("Executing: "+args);
        try (URLClassLoader loader = new URLClassLoader(new URL[]{new File(localPreLoadedJarPath).toURI().toURL()})) {
            Class<?> loadedClass= loader.loadClass(className);
            WorkerExeClass worker =(WorkerExeClass) loadedClass.getConstructor().newInstance();
            worker.execute(jobId,taskTrackerID,indexNumber);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
