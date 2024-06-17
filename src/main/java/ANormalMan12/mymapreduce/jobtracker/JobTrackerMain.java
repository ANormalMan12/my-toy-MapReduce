package ANormalMan12.mymapreduce.jobtracker;

import ANormalMan12.mymapreduce.network.rpcInterfaces.JobTrackerService;
import ANormalMan12.mymapreduce.network.rpcInterfaces.RmiServiceFactory;
import ANormalMan12.mymapreduce.network.rpcInterfaces.TaskTrackerService;
import ANormalMan12.mymapreduce.program.JobActivity;
import ANormalMan12.mymapreduce.program.JobConfig;
import ANormalMan12.mymapreduce.program.MapTaskInfo;
import ANormalMan12.mymapreduce.program.ReduceTaskInfo;
import ANormalMan12.mymapreduce.program.master.GlobalFileSplitInfo;
import ANormalMan12.mymapreduce.program.master.TaskInfo;
import ANormalMan12.mymapreduce.program.worker.Pair;
import ANormalMan12.mymapreduce.utils.UuidAllocator;
import ANormalMan12.mymapreduce.utils.configuration.LocalDataManager;
import ANormalMan12.mymapreduce.utils.configuration.RmiConfigs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class JobTrackerServiceImplServer  implements  JobTrackerService{

    @Override
    public synchronized String getIpByTaskTrackerId(UUID taskTrackerId) {
        return taskTrackerInfoHashMap.get(taskTrackerId).ipAddress;
    }

    @Override
    public synchronized  void finishReduceTask(UUID jobId, UUID workTaskTrackerId, int reduceTaskIndex, ArrayList<String> clientFetchPathList) throws RemoteException {
        String reduceTaskIP=taskTrackerInfoHashMap.get(workTaskTrackerId).ipAddress;
        jobActivityHashMap.get(jobId).finishReduceTask(workTaskTrackerId,reduceTaskIndex,reduceTaskIP,clientFetchPathList);
        try {
            executeTasksOnAvailableSlots();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized  JobActivity finishJobActivity(UUID jobId) throws RemoteException {
        JobActivity jobActivity=jobActivityHashMap.get(jobId);
        if(!jobActivity.isAllReduceTaskDone())return null;
        jobActivityHashMap.remove(jobId);
        return jobActivity;
    }

    @Override
    public synchronized  void finishMapTask(UUID jobId,UUID workTaskTrackerId,int mapTaskIndex, ArrayList<String> reduceFetchPathList) {
        String mapTaskIP=taskTrackerInfoHashMap.get(workTaskTrackerId).ipAddress;
        jobActivityHashMap.get(jobId).finishMapTask(workTaskTrackerId,mapTaskIndex,mapTaskIP,reduceFetchPathList);
        try {
            executeTasksOnAvailableSlots();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
        //TODO：目前假设TaskTracker检测到子进程正常退出时，会发送心跳信号，告诉JobTracker情况。
        //在TaskTracker没告知变更的情况下，不重新调配资源
    }

    @Override
    public synchronized  HashMap<Integer, MapTaskInfo> askMapTaskProgress(UUID jobId, ArrayList<Integer> undoneMapInputs) {
        HashMap<Integer,MapTaskInfo> mapTaskInfoHashMap=new HashMap<>();
        JobActivity jobActivity=jobActivityHashMap.get(jobId);
        for(int mapTaskIndex:undoneMapInputs){
            MapTaskInfo mapTaskInfo=jobActivity.getMapTask(mapTaskIndex);
            if(mapTaskInfo.isDone()){
            mapTaskInfoHashMap.put(mapTaskIndex,mapTaskInfo);
            }
        }
        return mapTaskInfoHashMap;
    }

    @Override
    public synchronized  boolean askJobProgress(UUID jobId) {
        JobActivity jobActivity=jobActivityHashMap.get(jobId);
        return jobActivity.isAllReduceTaskDone();
    }

    @Override
    public synchronized  int getR(UUID jobId) {
        return jobActivityHashMap.get(jobId).getR();
    }

    @Override
    public synchronized  int getM(UUID jobId) {
        return jobActivityHashMap.get(jobId).getM();
    }

    @Override
    public synchronized  Pair<UUID, String> getClientIdAddressByJobId(UUID jobId) {
        UUID clientID=jobActivityHashMap.get(jobId).getClientId();
        String clientIPaddr=clientIdAddressMap.get(clientID);
        return new Pair<>(clientID,clientIPaddr);
    }

    class TaskTrackerInfo{
        String ipAddress;
        TaskInfo[] taskInfos=new TaskInfo[3];//TODO: 单个节点能够承载的最大任务数
        long lastUpdateTime;
        TaskTrackerService taskTrackerService;

        @Override
        public synchronized  String toString() {
            return ipAddress;
        }

        public  TaskTrackerInfo(String ipAddress) {
            this.ipAddress=ipAddress;
            lastUpdateTime=System.currentTimeMillis();
        }
        public  synchronized void registerTaskTrackerService(UUID taskTrackerId) throws NotBoundException, RemoteException {
            taskTrackerService= RmiServiceFactory.getTaskTrackerService(
                    ipAddress,
                    taskTrackerId
            );
        }
        public synchronized void updateHeartBeatTime(){
            lastUpdateTime=System.currentTimeMillis();
        }
    }
    UuidAllocator clientIdAllocator=new UuidAllocator(),
            taskTrackerIdAllocator=new UuidAllocator(),
            jobIdAllocator=new UuidAllocator();
    ConcurrentHashMap<UUID,String> clientIdAddressMap=new ConcurrentHashMap<>();
    ConcurrentHashMap<UUID,TaskTrackerInfo> taskTrackerInfoHashMap=new ConcurrentHashMap<>();
    ConcurrentHashMap<UUID,JobActivity> jobActivityHashMap =new ConcurrentHashMap<>();
    JobTrackerServiceImplServer() throws RemoteException {
        Registry registry = LocateRegistry.createRegistry(RmiConfigs.RMI_REGISTER_PORT_JOB);
        JobTrackerService stub =
                (JobTrackerService) UnicastRemoteObject.exportObject(this, RmiConfigs.JOB_TRACKER_SERVICE_PORT);
        registry.rebind(RmiConfigs.JOB_TRACKER_SERVICE_NAME, stub);
        System.out.println("List services of JobTracker:");
        for(String s: registry.list()){
            System.out.println("/tService: "+s);
        }
    }

    @Override
    public synchronized GlobalFileSplitInfo getGlobalMapInputSplit(UUID jobId, int mapTaskIndex) throws RemoteException {
        JobActivity jobActivity =jobActivityHashMap.get(jobId);
        return jobActivity.getMapSplitInfo(mapTaskIndex);
    }

    @Override
    public synchronized UUID registerClient(String clientIP) throws RemoteException {
        System.out.println("Try to allocate client ID");
        UUID clientId = clientIdAllocator.allocateUUID();
        System.out.println("New Client ID:"+clientId.toString());
        clientIdAddressMap.put(clientId,clientIP);
        return clientId;
    }

    @Override
    public synchronized String queryTaskProgress(UUID jobId) throws RemoteException {
        JobActivity activity= jobActivityHashMap.get(jobId);
        if(activity==null){
            return "";
        }
        return activity.toString();
    }

    @Override
    public synchronized UUID submitJob(UUID clientID, JobConfig jobConfig) throws RemoteException {
        UUID jobId= jobIdAllocator.allocateUUID();//添加到表项
        System.out.println("New Job Arrived"+jobId.toString());
        JobActivity jobToSubmit=new JobActivity(clientID,jobConfig);
        jobActivityHashMap.put(jobId,jobToSubmit);
        System.out.println("New Job has been stored"+jobId.toString());
        executeTasksOnAvailableSlots();
        return jobId;
    }

    synchronized void executeTasksOnAvailableSlots()throws  RemoteException{
        ArrayList<Pair<UUID,Integer>> slotArrayList=new ArrayList<>();

        System.out.println("Begin to check available slots");
        for(UUID taskTrackerId:taskTrackerInfoHashMap.keySet()){
            TaskTrackerInfo taskTrackerInfo=taskTrackerInfoHashMap.get(taskTrackerId);
            for(int iTask=0;iTask<taskTrackerInfo.taskInfos.length;++iTask){
                TaskInfo taskInfo= taskTrackerInfo.taskInfos[iTask];
                if(taskInfo==null||taskInfo.isDone()){
                    taskTrackerInfo.taskInfos[iTask]=null;
                    slotArrayList.add(new Pair<>(taskTrackerId,iTask));
                }
            }
            //if(taskTrackerInfo.lastUpdateTime-System.currentTimeMillis()>JOBTRACKER_MISSED_MS_TIME){
            //TODO    throw new RuntimeException("TaskTracker超时了");
            //}
        }
        System.out.println("Number of all available slots:"+slotArrayList.size());
        int usedSlot=0;
        for(UUID jobId:jobActivityHashMap.keySet()){
            //遍历所有未提交任务
            JobActivity jobActivity = jobActivityHashMap.get(jobId);
            ArrayList<MapTaskInfo> mapTaskInfoList=jobActivity.getNotRunningMapTask(slotArrayList.size());
            for(MapTaskInfo mapTaskInfoToRun:mapTaskInfoList){
                if(usedSlot>=slotArrayList.size()){
                    System.out.println("执行Map任务后，使用了槽位"+usedSlot+"个");
                    return;
                }
                Pair<UUID,Integer> freeSlot=slotArrayList.get(usedSlot);
                usedSlot++;
                TaskTrackerInfo  taskTrackerInfo =  taskTrackerInfoHashMap.get(freeSlot.getKey());
                System.out.println("Distribute Map Work"+taskTrackerInfo.toString());
                mapTaskInfoToRun.setRunning();
                taskTrackerInfo.taskTrackerService.executeMapTask(
                    jobId,mapTaskInfoToRun.getTaskIndex(),jobActivity.getJarPath(),jobActivity.getMapperClassName()
                );
            }
            ArrayList<ReduceTaskInfo> reduceTaskInfoList=jobActivity.getNotRunningReduceTask(slotArrayList.size());
            for(ReduceTaskInfo reduceTaskInfoToRun:reduceTaskInfoList){
                if(usedSlot>=slotArrayList.size()){
                    System.out.println("执行Map任务后，使用了槽位"+usedSlot+"个");
                    return;
                }
                Pair<UUID,Integer> freeSlot=slotArrayList.get(usedSlot);
                usedSlot++;
                TaskTrackerInfo  taskTrackerInfo =  taskTrackerInfoHashMap.get(freeSlot.getKey());
                System.out.println("Distribute Reduce Work"+taskTrackerInfo.toString());
                reduceTaskInfoToRun.setRunning();
                taskTrackerInfo.taskTrackerService.executeReduceTask(
                        jobId,reduceTaskInfoToRun.getTaskIndex(),jobActivity.getJarPath(),jobActivity.getReducerClassName()
                );
            }
        }
        System.out.println("执行Map任务后，使用了槽位"+usedSlot+"个");
    }

    @Override
    public synchronized UUID registerTaskTracker(String taskTrakcerAddr)  throws RemoteException {
        System.out.println("Have been asked to register a taskTracker");
        UUID ttid=taskTrackerIdAllocator.allocateUUID();
        System.out.println("ttid:"+ttid.toString());
        taskTrackerInfoHashMap.put(ttid,new TaskTrackerInfo(taskTrakcerAddr));
        System.out.println(String.format("Register TaskTracker：Address：%s",taskTrakcerAddr));
        return ttid;
    }
    @Override
    public boolean sendTaskTrackerHeartBeat(UUID ttID) throws RemoteException {
        TaskTrackerInfo taskTrackerInfo=taskTrackerInfoHashMap.get(ttID);
        if(taskTrackerInfo.taskTrackerService ==null){
            //TODO: 如果要增加容错，应该添加新逻辑
            try {
                taskTrackerInfo.registerTaskTrackerService(ttID);
            } catch (NotBoundException e) {
                System.out.println("启动TaskTracker服务失败");
                e.printStackTrace();
            }
        }
        //TODO: 增加容错时考虑：System.out.println(String.format("Heart beat by "+ttID.toString()+": %s",TimeUtils.getTimeString()));
        return true;
    }
}
public class JobTrackerMain  {
    /*
    public static void runClient(){
        ClientMainWordCount.runMainProcess(
                "wwy-VirtualBox",
                "/home/wwy/wwyMapReduceProgram/word_count_test.txt",
                "/home/wwy/wwyMapReduceProgram/wwyMapReduce.jar"
        );
    }
*/
    public static void main(String[] args)  {
        try {
                System.out.println("JobTracker: Launching "+ LocalDataManager.version);
                //-------------------------------------------------------------
                //String name = RmiConfigs.JOB_TRACKER_SERVICE_NAME;
                //LocalLogSysOutputReplacement.replaceOutErrStream(name);
                JobTrackerService jobTrackerServer = new JobTrackerServiceImplServer();
            //        System.setProperty("java.rmi.server.hostname","192.168.56.101");
                //-------------------------------------------------------------
                //runClient();
                for (int i = 0;true;++i){
                    System.out.printf("Job Tracker is running: %d\n",i);
                    Thread.sleep(5000);
                }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
