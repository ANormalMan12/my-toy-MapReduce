package ANormalMan12.mymapreduce.program.worker;

import ANormalMan12.mymapreduce.network.rpcInterfaces.ClientService;
import ANormalMan12.mymapreduce.network.rpcInterfaces.JobTrackerService;
import ANormalMan12.mymapreduce.network.rpcInterfaces.RmiServiceFactory;
import ANormalMan12.mymapreduce.program.master.GlobalFileSplitInfo;
import ANormalMan12.mymapreduce.utils.configuration.LocalDataManager;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

public abstract class Mapper<
        K2 extends Comparable<K2> & Serializable,
        V2 extends Comparable<V2> & Serializable> implements WorkerExeClass{
    public abstract void map(Integer key,String value,MRcontext<K2,V2> context);

    public void execute(UUID jobId,UUID taskTrackerId, int indexNumber)throws Exception{
        System.out.println("Begin to execute Mapper: "+indexNumber);
        //从JobTracker获取负责的数据块的网络-机器位置等信息
        JobTrackerService jobTrackerService = RmiServiceFactory.getJobTrackerService();
        int R=jobTrackerService.getR(jobId);
        System.out.println("Mapper: Found Reduce Task Number:"+R);
        Pair<UUID,String > clientIdAddr= jobTrackerService.getClientIdAddressByJobId(jobId);
        ClientService clientService =RmiServiceFactory.getClientService(clientIdAddr.val,clientIdAddr.key);
        //读取Client处的输入数据
        GlobalFileSplitInfo fileSplitInfo=jobTrackerService.getGlobalMapInputSplit(jobId,indexNumber);
        byte[] inputData=clientService.downloadFileSplit(fileSplitInfo.filePath,fileSplitInfo.fileSplitInfo);
        System.out.println("Data has arrived");
        //输入不保存为文件，直接以流的形式输入，并运行
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputData);
        InputStreamReader inputStream = new InputStreamReader(byteArrayInputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader=new BufferedReader(inputStream);
        MRcontext<K2,V2> mapContext=new MRcontext(R);
        String line;
        for(int i=0;(line=bufferedReader.readLine())!=null;++i){
            if(i==0){
                System.out.println("Map: First to read:"+line);
            }
            map(i,line,mapContext);
        }
        //将结果写入文件
        ArrayList<byte[]> resultByteList= mapContext.getRListsOfBytes();
        ArrayList<String> reduceFetchPathList=new ArrayList<>();
        for(int iR=0;iR<R;++iR){
            String mapResultLocalPath=LocalDataManager.writeToLocalFile(
                    LocalDataManager.MAP_RESULT_DIRNAME,
                    String.format(jobId+"-%d-%d",indexNumber,iR),
                    resultByteList.get(iR)
            ).toAbsolutePath().toString();
            System.out.println("Write to:"+mapResultLocalPath);
            reduceFetchPathList.add(mapResultLocalPath);
        }
        jobTrackerService.finishMapTask(jobId,taskTrackerId,indexNumber,reduceFetchPathList);
    }
}
