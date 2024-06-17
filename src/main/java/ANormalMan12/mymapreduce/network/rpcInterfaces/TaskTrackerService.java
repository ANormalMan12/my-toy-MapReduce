package ANormalMan12.mymapreduce.network.rpcInterfaces;


import ANormalMan12.mymapreduce.program.master.FileSplitInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

public interface TaskTrackerService extends Remote {
    byte[] downloadFileSplit(String filePath, FileSplitInfo splitInfo) throws  RemoteException;
    byte[] downloadWholeFile(String filePath)throws  RemoteException;

    /*需要及时返回*/
    boolean executeMapTask(UUID jobId,int mapTaskIndex,String localPreLoadedJarPath,String mapperName)throws  RemoteException;
    boolean executeReduceTask(UUID jobId,int reduceTaskIndex,String localPreLoadedJarPath,String reducerName)throws  RemoteException;
}
