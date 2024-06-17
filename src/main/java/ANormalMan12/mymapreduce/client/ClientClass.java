package ANormalMan12.mymapreduce.client;

import ANormalMan12.mymapreduceApp.WordCount.WordCountMapper;
import ANormalMan12.mymapreduceApp.WordCount.WordCountReducer;
import ANormalMan12.mymapreduce.network.rpcInterfaces.RmiServiceFactory;
import ANormalMan12.mymapreduce.network.rpcInterfaces.TaskTrackerService;
import ANormalMan12.mymapreduce.program.JobActivity;
import ANormalMan12.mymapreduce.program.JobConfig;
import ANormalMan12.mymapreduce.program.ReduceTaskInfo;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class ClientClass {

    public static void main(String[] args) throws Exception {
        //TODO：需要实现将输入文件和jar拷贝到缓存区目录的操作。
        if(args.length!=3){
            System.out.println("非法数量的参数，无法启动");
        }
        String clientIPaddress=args[0];
        String inputFilePath=args[1];
        String jarPath = args[2];
        String mapperClassName= WordCountMapper.class.getName();
        String reduceClassName= WordCountReducer.class.getName();
        int M=5;
        int R=5;
        runClient(clientIPaddress,inputFilePath,jarPath,mapperClassName,reduceClassName,M,R);
    }
    public static void runClient(
            String clientIPaddress,
            String inputFilePath,
            String jarPath ,
            String mapperClassName,
            String reduceClassName,
            int M,int R
        )throws  Exception{
        System.out.println("Mapper-Name:"+mapperClassName);
        System.out.println("Reduce-Name:"+reduceClassName);
        ClientServiceImpl clientService=new ClientServiceImpl(
                clientIPaddress
        );
        JobConfig job=new JobConfig(
                mapperClassName,
                reduceClassName,
                jarPath,
                M,
                R,
                inputFilePath,
                clientIPaddress
        );
        UUID jobId=clientService.submitJob(job);
        for(int i=0;!clientService.isAllDone(jobId);i++){
            System.out.println("Waiting:"+i);
            Thread.sleep(5000);
        }
        JobActivity jobActivity=clientService.jobTrackerService.finishJobActivity(jobId);
        System.out.println("正在打印结果");
        for(int iR=0;iR<R;++iR) {
            ReduceTaskInfo reduceTaskInfo =jobActivity.getReduceTask(iR);
            String outputFile=reduceTaskInfo.getOutputFilePaths().get(0);
            UUID outputTaskTrackerId=reduceTaskInfo.getExecutorTaskTrackerId();
            String outputTaskTrackerIp=clientService.jobTrackerService.getIpByTaskTrackerId(outputTaskTrackerId);
            TaskTrackerService targetTaskTrackerService=RmiServiceFactory.getTaskTrackerService(outputTaskTrackerIp,outputTaskTrackerId);
            byte[] data=targetTaskTrackerService.downloadWholeFile(outputFile);
            System.out.println("-----------Result: Reduce Task "+iR+"-----------");
            writeToStdByByte(data);
        }
        System.out.println("Client已完成");
    }
    public static void writeToStdByByte(byte[]inputData){
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputData);
        InputStreamReader inputStream = new InputStreamReader(byteArrayInputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader=new BufferedReader(inputStream);
        try{
            String line;
            for(int i=0;(line=bufferedReader.readLine())!=null;++i){
                System.out.println(line);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
