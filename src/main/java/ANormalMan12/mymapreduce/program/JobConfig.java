package ANormalMan12.mymapreduce.program;

import ANormalMan12.mymapreduce.program.master.GlobalFileSplitInfo;
import ANormalMan12.mymapreduce.program.master.LineInputFormat;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class JobConfig implements Serializable {
    String mapperClassName;
    String reducerClassName;
    String jarPath;
    List<GlobalFileSplitInfo> mapInputSplits;
    int mapTaskNumber;
    int reduceTaskNumber;
    public JobConfig(
        String mapperClassName,
        String reducerClassName,
        String jarPath,
        int mapTaskNumber,
        int reduceTaskNumber,
        String inputFilePath,
        String clientIPaddress
    ) throws IOException {
        this.mapperClassName=mapperClassName;
        this.reducerClassName=reducerClassName;
        this.jarPath=jarPath;
        this.mapTaskNumber=mapTaskNumber;
        this.reduceTaskNumber=reduceTaskNumber;
        List<GlobalFileSplitInfo> globalFileSplitInfoList= LineInputFormat
                .getSplitList(inputFilePath,mapTaskNumber)
                .stream()
                .map(x->new GlobalFileSplitInfo(clientIPaddress,inputFilePath,x))
                .collect(Collectors.toList());
        this.mapInputSplits=globalFileSplitInfoList;
        for(GlobalFileSplitInfo globalFileSplitInfo:globalFileSplitInfoList){
            System.out.println(globalFileSplitInfo);
        }
    }

}