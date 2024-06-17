package ANormalMan12.mymapreduce.network.rpcInterfaces;

import ANormalMan12.mymapreduce.program.master.FileSplitInfo;
import ANormalMan12.mymapreduce.program.master.GlobalFileSplitInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientService extends Remote {
    byte[] downloadFileSplit(String filePath,FileSplitInfo splitInfo) throws  RemoteException;
    byte[] downloadWholeFile(String filePath)throws  RemoteException;
}
