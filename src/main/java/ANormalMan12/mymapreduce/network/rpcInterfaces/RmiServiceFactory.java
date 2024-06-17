package ANormalMan12.mymapreduce.network.rpcInterfaces;

import ANormalMan12.mymapreduce.utils.configuration.RmiConfigs;
import ANormalMan12.mymapreduce.utils.configuration.NetworkManager;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.UUID;

public class RmiServiceFactory {
    public static JobTrackerService getJobTrackerService() throws RemoteException, NotBoundException {
        while(true) {
            try {
                Registry registry = LocateRegistry.getRegistry(
                        NetworkManager.getJobTrackerHostName(),
                        RmiConfigs.RMI_REGISTER_PORT_JOB);
                return (JobTrackerService) registry.lookup(
                        RmiConfigs.JOB_TRACKER_SERVICE_NAME
                );
            } catch (Exception e) {
                System.out.println("尝试获取jobTracker失败，正在重试");
            }
        }
    }
    public static ClientService getClientService(String clientIPAddr, UUID clientID) throws RemoteException, NotBoundException {
        while(true) {
            try {
                Registry registry = LocateRegistry.getRegistry(
                        clientIPAddr,
                        RmiConfigs.RMI_REGISTER_PORT_CT);
                return (ClientService) registry.lookup(
                    clientID.toString()
                );
            } catch (Exception e) {
                System.out.println("尝试获取Client Server"+clientID+":"+clientIPAddr +"失败，正在重试");
            }
        }
    }
    public static TaskTrackerService getTaskTrackerService(String taskTrackerIPAddr, UUID taskTrackerID) throws RemoteException, NotBoundException {
        while(true) {
            try {
                Registry registry = LocateRegistry.getRegistry(
                        taskTrackerIPAddr,
                        RmiConfigs.RMI_REGISTER_PORT_CT);
                return (TaskTrackerService) registry.lookup(
                        taskTrackerID.toString()
                );
            } catch (Exception e) {
                System.out.println("尝试获取Task Tracker Server"+taskTrackerID+":"+taskTrackerIPAddr +"失败，正在重试");
            }
        }
    }
}
