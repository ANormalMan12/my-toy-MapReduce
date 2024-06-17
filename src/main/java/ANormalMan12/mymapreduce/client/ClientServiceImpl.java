package ANormalMan12.mymapreduce.client;

import ANormalMan12.mymapreduce.network.rpcInterfaces.ClientService;
import ANormalMan12.mymapreduce.network.rpcInterfaces.JobTrackerService;
import ANormalMan12.mymapreduce.network.rpcInterfaces.RmiServiceFactory;
import ANormalMan12.mymapreduce.program.JobConfig;
import ANormalMan12.mymapreduce.program.master.FileSplitInfo;
import ANormalMan12.mymapreduce.program.master.LineInputFormat;
import ANormalMan12.mymapreduce.utils.configuration.RmiConfigs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.UUID;

public class ClientServiceImpl implements ClientService {
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
    String clientIPaddress;
    JobTrackerService jobTrackerService;
    UUID clientID;
    public ClientServiceImpl(String clientIPaddress) throws Exception{
        this.clientIPaddress=clientIPaddress;
        System.out.println("My IP Addr:"+clientIPaddress);
        //尝试获取jobTracker服务。
        jobTrackerService = RmiServiceFactory.getJobTrackerService();
        System.out.println("Successfully get Job Tracker");
        //正在获取clientID
        clientID=jobTrackerService.registerClient(clientIPaddress);

        Registry registry = LocateRegistry.createRegistry(RmiConfigs.RMI_REGISTER_PORT_CT);
        ClientService stub =
                (ClientService)  UnicastRemoteObject.exportObject(this, RmiConfigs.CLIENT_SERVICE_PORT);
        registry.rebind(clientID.toString(), stub);//Use Client ID as RMI name
        System.out.println("Client RMI Interface Finished Initialization");
    }
    public boolean isAllDone(UUID jobId)throws  RemoteException{
        return jobTrackerService.askJobProgress(jobId);
    }
    public UUID submitJob(JobConfig jobConf){
        try {
            return jobTrackerService.submitJob(clientID,jobConf);
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }
}
