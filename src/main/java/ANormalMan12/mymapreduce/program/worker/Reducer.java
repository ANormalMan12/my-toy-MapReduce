package ANormalMan12.mymapreduce.program.worker;

import ANormalMan12.mymapreduce.network.rpcInterfaces.JobTrackerService;
import ANormalMan12.mymapreduce.network.rpcInterfaces.RmiServiceFactory;
import ANormalMan12.mymapreduce.network.rpcInterfaces.TaskTrackerService;
import ANormalMan12.mymapreduce.program.MapTaskInfo;
import ANormalMan12.mymapreduce.utils.ObjectStreamUtils;
import ANormalMan12.mymapreduce.utils.configuration.LocalDataManager;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;

public abstract class Reducer<
        K2 extends Comparable<K2> & Serializable,
        V2 extends Comparable<V2> & Serializable,
        K3 extends Comparable<K3> & Serializable,
        V3 extends Comparable<V3> & Serializable> implements WorkerExeClass{
    public abstract void reduce(K2 key, Iterator<V2> valueIterator, MRcontext<K3,V3> context);

    byte[][] allMapResult;
    int M;
    ArrayList<Integer> getNotReceivedMapTasks(){
        ArrayList<Integer> getReceivedList=new ArrayList<>();
        for(int i=0;i<M;++i){
            if(allMapResult[i]==null){
                getReceivedList.add(i);
            }
        }
        if(getReceivedList.size()==0){
            getReceivedList=null;
        }
        return getReceivedList;
    }

    public byte[] getMapTaskResult(String dataIP,UUID dataTaskTrackerId,String mapResultPath) throws NotBoundException, RemoteException {
        TaskTrackerService taskTrackerService =RmiServiceFactory.getTaskTrackerService(dataIP,dataTaskTrackerId);
        return taskTrackerService.downloadWholeFile(mapResultPath);
    }

    public void execute(UUID jobId,UUID taskTrackerId,int indexNumber) throws Exception{
        System.out.println("Begin to execute Reduce Task "+indexNumber);
        JobTrackerService jobTrackerService= RmiServiceFactory.getJobTrackerService();
        int ir=indexNumber;
        M=jobTrackerService.getM(jobId);
        allMapResult=new byte[M][];
        ArrayList<Integer> undoneMapInputs;
        while((undoneMapInputs=getNotReceivedMapTasks())!=null){
            HashMap<Integer,MapTaskInfo> mapTaskInfoHashMap=jobTrackerService.askMapTaskProgress(jobId,undoneMapInputs);
            for(int Mindex:mapTaskInfoHashMap.keySet()){
                MapTaskInfo mapTaskInfo=mapTaskInfoHashMap.get(Mindex);
                assert(Mindex==mapTaskInfo.getTaskIndex());
                UUID resultTaskTrackerId=mapTaskInfo.getExecutorTaskTrackerId();
                String mapResultPath= mapTaskInfo.getOutputFilePaths().get(ir);
                String ipAddr=jobTrackerService.getIpByTaskTrackerId(resultTaskTrackerId);
                allMapResult[Mindex]=getMapTaskResult(ipAddr,resultTaskTrackerId,mapResultPath);
            }
            Thread.sleep(5000);//每5s轮询是否有新的结束的Map任务
        }
        System.out.println("All Map Inputs have been collected");

        ObjectStreamUtils<Pair<K2,V2>> readWriter =new ObjectStreamUtils<>();
        HashMap<K2,ArrayList<V2>> k2V2HashMap=new HashMap<>();
        for(int i=0;i<M;++i){
            List<Pair<K2,V2>> inputListI= readWriter.readObjectFromByteStream(allMapResult[i]);
            for(Pair<K2,V2> k2V2Pair:inputListI){
                //if(i==0) {
                //    System.out.print(k2V2Pair+";");
                //}
                if (!k2V2HashMap.containsKey(k2V2Pair.key)) {
                    k2V2HashMap.put(k2V2Pair.key,new ArrayList<>());
                }
                k2V2HashMap.get(k2V2Pair.key).add(k2V2Pair.val);

            }
        }
        MRcontext<K3,V3> resultContext=new MRcontext<>(1);
        for(K2 key:k2V2HashMap.keySet()){
            reduce(key,k2V2HashMap.get(key).iterator(),resultContext);
        }
        byte[] resultFileContent=resultContext.getBytesDataInUtf8();
        String writeTo=LocalDataManager.writeToLocalFile(
                LocalDataManager.REDUCE_RESULT_DIRNAME,
                String.format("part-r-%04d", ir),
                resultFileContent
        ).toAbsolutePath().toString();
        ArrayList<String> resultPaths=new ArrayList<>();
        resultPaths.add(writeTo);
        jobTrackerService.finishReduceTask(jobId,taskTrackerId,ir,resultPaths);
        System.out.println("Result written to"+writeTo);
    }
}
