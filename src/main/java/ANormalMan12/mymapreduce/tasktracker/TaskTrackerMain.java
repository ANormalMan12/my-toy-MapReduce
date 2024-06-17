package ANormalMan12.mymapreduce.tasktracker;


import ANormalMan12.mymapreduce.network.rpcInterfaces.JobTrackerService;
import ANormalMan12.mymapreduce.network.rpcInterfaces.RmiServiceFactory;
import ANormalMan12.mymapreduce.network.rpcInterfaces.TaskTrackerService;
import ANormalMan12.mymapreduce.program.master.FileSplitInfo;
import ANormalMan12.mymapreduce.program.master.LineInputFormat;
import ANormalMan12.mymapreduce.utils.configuration.LocalDataManager;
import ANormalMan12.mymapreduce.utils.configuration.NetworkManager;
import ANormalMan12.mymapreduce.utils.configuration.RmiConfigs;
import ANormalMan12.mymapreduce.utils.RunJava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


class TaskTrackerServiceImplServer implements TaskTrackerService{
    String taskTrackerAddress;//初始化于构造函数
    JobTrackerService jobTrackerService;//初始化于构造函数
    UUID taskTrackerId;//初始化于构造函数

    @Override
    public byte[] downloadFileSplit(String filePath, FileSplitInfo splitInfo) throws RemoteException {
        return LineInputFormat.getByteArrayBySplitInfoUtf8(filePath,splitInfo);
    }
    @Override
    public byte[] downloadWholeFile(String filePath) throws RemoteException {
        try {
            return Files.readAllBytes(Paths.get(filePath));
        } catch (IOException e) {
            return null;
        }
    }
    @Override
    public boolean executeReduceTask(UUID jobId, int reduceTaskIndex,String localPreLoadedJarPath, String reducerName) {
        return executeNewTask(localPreLoadedJarPath,reducerName,reduceTaskIndex,jobId);
    }

    @Override
    public boolean executeMapTask(UUID jobId,int mapTaskIndex,String localPreLoadedJarPath, String mapperName) {
        return executeNewTask(localPreLoadedJarPath,mapperName,mapTaskIndex,jobId);
    }
    public boolean executeNewTask(
            String localPreLoadedJarPath,
            String className,
            int taskIndex,
            UUID jobId
    ){
        //Arguments begin here
        System.out.println("Task Tracker Received Requirements from JobTracker");
        System.out.println("Arguments"+localPreLoadedJarPath+className+taskIndex+jobId.toString());
        List<String> argList=new ArrayList<>();
        argList.add(localPreLoadedJarPath);
        argList.add("ANormalMan12.mymapreduce.tasktracker.Worker");
        argList.add(localPreLoadedJarPath);
        argList.add(className);
        argList.add(String.valueOf(taskIndex));
        argList.add(jobId.toString());
        argList.add(taskTrackerId.toString());
        ProcessBuilder builder = new ProcessBuilder(RunJava.getRunJavaArgList(argList));
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        Thread processThread = new Thread(() -> {
            try {
                Process taskProcess = builder.start();
                taskProcess.waitFor();
                System.out.println("Process exited with code " + taskProcess.exitValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        try {
            processThread.start();
            return true; // Start成功即返回true
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 初始化TaskTracker主服务
     * @param taskTrackerHostName
     * @throws RemoteException
     * @throws NotBoundException
     * @throws InterruptedException
     */
    TaskTrackerServiceImplServer(String taskTrackerHostName) throws Exception{
        this.taskTrackerAddress =taskTrackerHostName;//初始化名称
        this.taskTrackerId=registerRMIJobTracker();//登记JobTracker
        System.out.println("Successful Connection with JobTracker");
        try {
            Registry registry = LocateRegistry.createRegistry(RmiConfigs.RMI_REGISTER_PORT_CT);
            TaskTrackerService stub =
                    (TaskTrackerService) UnicastRemoteObject.exportObject(this, RmiConfigs.TASK_TRACKER_SERVICE_PORT);
            registry.rebind(this.taskTrackerId.toString(), stub);//服务的名称是taskTrackerId
            System.out.println("TaskTracker RMI Service Online now");
            System.out.println("List services of TaskTracker:");
            for(String s: registry.list()){
                System.out.println("\tService: "+s);
            }
        } catch (RemoteException e) {
            System.err.println("TaskTracker: Failed Initialization");
            throw(e);
        }//TODO：此处有一个时序BUG问题，应该等TaskTracker充分启动后再提交任务。
        System.out.println("RMI Initialized ");
        while(true){
            try{
                for (int i = 0; true; ++i) {
                    jobTrackerService.sendTaskTrackerHeartBeat(taskTrackerId);
                    //开始发送心跳信号
                    System.out.printf("Task Tracker is running: %d\n", i);
                    Thread.sleep(NetworkManager.TASKTRACKER_REPEATED_MS_TIME);
                }
            }catch (Exception e){
                for(int i=0;true;++i) {
                    System.out.println("Connection Failed: "+i+" retry");
                    this.taskTrackerId = registerRMIJobTracker();
                }
            }
        }
    }

    /**
     * 尝试连接JobTracker。
     * @return
     * @throws RemoteException
     * @throws NotBoundException
     * @throws InterruptedException
     */
    public UUID registerRMIJobTracker() throws RemoteException, NotBoundException, InterruptedException {
        for(int i=0;true;++i) {
            try {
                System.out.println(i + ":TaskTracker tries to connect to JobTracker");
                this.jobTrackerService = RmiServiceFactory.getJobTrackerService();
                System.out.println("Have got jobTracker Service");
                return jobTrackerService.registerTaskTracker(taskTrackerAddress);
            }catch (Exception e){
                System.out.println("Fail to connect to JobTracker:"+i+" try time");
                Thread.sleep(5000);
            }
        }
    }
}
public class TaskTrackerMain {
    public static void main(String[] args)  {
        String ipAddr = args[0];
        System.out.println("WWY MapReduce Version: "+ LocalDataManager.version);
        System.out.println("Initializing in IP: "+ipAddr);
        //-------------------------------------------------------------
        //LocalLogSysOutputReplacement.replaceOutErrStream(name);
        try {
            TaskTrackerServiceImplServer taskTrackerServer = new TaskTrackerServiceImplServer(ipAddr);
        } catch (Exception e) {
             e.printStackTrace();
        }
    }
}

