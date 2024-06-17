package ANormalMan12.mymapreduce.program;

import ANormalMan12.mymapreduce.program.master.GlobalFileSplitInfo;
import ANormalMan12.mymapreduce.program.master.TaskInfo;
import javafx.concurrent.Task;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.indexOfSubList;
import static java.util.Collections.synchronizedList;


public class JobActivity implements Serializable {
    public MapTaskInfo getMapTask(int iM) {
        return mapTaskInfoList.get(iM);
    }

    public String getMapperClassName() {
        return mapperClassName;
    }
    public String getReducerClassName(){
        return reducerClassName;
    }
    public String getJarPath() {
        return jarPath;
    }

    public boolean isAllReduceTaskDone(){
        for(ReduceTaskInfo reduceTaskInfo:reduceTaskInfoList){
            if(!reduceTaskInfo.isDone()){
                return false;
            }
        }
        return true;
    }
    public ReduceTaskInfo getReduceTask(int iR){
        return reduceTaskInfoList.get(iR);
    }
    public ArrayList<MapTaskInfo> getNotRunningMapTask(int K) {
        ArrayList<MapTaskInfo> arrayList = new ArrayList<>();
        System.out.println("Check Map Tasks:");
        for (int i = 0; i < mapTaskInfoList.size(); ++i) {
            if (mapTaskInfoList.get(i).isNotRunning()) {
                arrayList.add(mapTaskInfoList.get(i));
                System.out.printf("%d: 待启动;",i);
                if (arrayList.size() >= K) {
                    return arrayList;
                }
            }
            System.out.printf("%d: done;",i);
        }
        return arrayList;
    }
    public ArrayList<ReduceTaskInfo> getNotRunningReduceTask(int K) {
        ArrayList<ReduceTaskInfo> arrayList = new ArrayList<>();
        System.out.println("Check Reduce Tasks:");
        for (int i = 0; i < reduceTaskInfoList.size(); ++i) {
            if (reduceTaskInfoList.get(i).isNotRunning()) {
                arrayList.add(reduceTaskInfoList.get(i));
                System.out.printf("%d: 待启动;",i);
                if (arrayList.size() >= K) {
                    return arrayList;
                }
            }
        }
        return arrayList;
    }
    @Override
    public String toString() {
        return "Not Implemented Function";
    }
    public void finishMapTask(UUID workTaskTrackerId,int mapTaskIndex,String mapTaskIP, ArrayList<String> reduceFetchPathList){
        MapTaskInfo mapTaskInfo = mapTaskInfoList.get(mapTaskIndex);
        mapTaskInfo.setDone(workTaskTrackerId,mapTaskIP,reduceFetchPathList);
    }
    public void finishReduceTask(UUID workTaskTrackerId,int reduceTaskIndex,String reduceTaskIP, ArrayList<String> clientFetchList){
        ReduceTaskInfo reduceTaskInfo = reduceTaskInfoList.get(reduceTaskIndex);
        reduceTaskInfo.setDone(workTaskTrackerId,reduceTaskIP,clientFetchList);
    }
    public int getM() {
        return M;
    }

    public int getR() {
        return R;
    }



    final List<MapTaskInfo> mapTaskInfoList ;
    final List<ReduceTaskInfo> reduceTaskInfoList;
    final int R,M;
    final String jarPath;
    final String mapperClassName,reducerClassName;
    UUID clientId;
    public JobActivity(UUID clientId,JobConfig jobConfig){
        this.clientId=clientId;
        mapperClassName=jobConfig.mapperClassName;
        reducerClassName=jobConfig.reducerClassName;
        R=jobConfig.reduceTaskNumber;
        M=jobConfig.mapTaskNumber;
        jarPath=jobConfig.jarPath;
        //MapTask Initialization
        ArrayList<MapTaskInfo> unsafeMapList=new ArrayList<>();
        assert(jobConfig.mapInputSplits.size()==M);
        for(int i=0;i<M;++i){
            GlobalFileSplitInfo globalFileSplitInfo=jobConfig.mapInputSplits.get(i);
            unsafeMapList.add(new MapTaskInfo(
                    i,globalFileSplitInfo
             ));
        }
        mapTaskInfoList= synchronizedList(unsafeMapList);
        //Reduce Task Initialization
        ArrayList<ReduceTaskInfo> unsafeReduceList=new ArrayList<>();
        for(int iR=0;iR<R;++iR){
            unsafeReduceList.add(new ReduceTaskInfo(iR));
        }
        reduceTaskInfoList=synchronizedList(unsafeReduceList);
    }
    public GlobalFileSplitInfo getMapSplitInfo(int mapTaskIndex){
        return mapTaskInfoList.get(mapTaskIndex).getInputGLobalFileSplit();
    }

    public UUID getClientId() {
        return clientId;
    }
}
