package ANormalMan12.mymapreduce.network.rpcInterfaces;

import ANormalMan12.mymapreduce.program.JobActivity;
import ANormalMan12.mymapreduce.program.JobConfig;
import ANormalMan12.mymapreduce.program.MapTaskInfo;
import ANormalMan12.mymapreduce.program.master.GlobalFileSplitInfo;
import ANormalMan12.mymapreduce.program.worker.Pair;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public interface JobTrackerService extends Remote{
    public UUID submitJob(UUID clientID,JobConfig jobConfig)throws RemoteException;
    public void finishMapTask(UUID jobId,UUID workTaskTrackerId,int mapTaskIndex, ArrayList<String> reduceFetchPathList)throws  RemoteException;

    public void finishReduceTask(UUID jobId,UUID workTaskTrackerId,int reduceTaskIndex, ArrayList<String> clientFetchPathList)throws  RemoteException;
    public JobActivity finishJobActivity(UUID jobId) throws  RemoteException;
    public boolean sendTaskTrackerHeartBeat(UUID tasktrackerID) throws RemoteException;
    public String getIpByTaskTrackerId(UUID taskTrackerId)throws  RemoteException;
    public HashMap<Integer,MapTaskInfo> askMapTaskProgress(UUID jobId, ArrayList<Integer>undoneMapInputs)throws  RemoteException;
    public boolean askJobProgress(UUID jobId)throws  RemoteException;
    public GlobalFileSplitInfo getGlobalMapInputSplit(UUID jobId,int mapTaskIndex) throws RemoteException;
    public Pair<UUID,String> getClientIdAddressByJobId(UUID jobId) throws RemoteException;
    public String queryTaskProgress(UUID jobId) throws RemoteException;
    public UUID registerTaskTracker(String taskTrakcerIP)throws RemoteException;
    public UUID registerClient(String clientIP) throws RemoteException;
    public int getR(UUID jobId)throws  RemoteException;
    public int getM(UUID jobId)throws  RemoteException;
}
