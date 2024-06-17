package ANormalMan12.mymapreduce.program.master;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

public class TaskInfo implements Serializable {
    final int index;
    TaskSituation taskSituation=TaskSituation.submitted;
    ArrayList<String> outputFilePathList;
    String executorTaskTrackerIpIPaddr;
    UUID executorTaskTrackerId;
    public boolean isNotRunning(){
        return taskSituation==TaskSituation.submitted;
    }
    public boolean isDone(){
        return taskSituation==TaskSituation.done;
    }
    public TaskInfo(int index) {
        this.index=index;
    }
    public int getTaskIndex(){
        return index;
    }
    public synchronized void setRunning(){
        taskSituation=TaskSituation.running;
    }
    public synchronized void setDone(UUID executorTaskTrackerId,String executorTaskTrackerIp,ArrayList<String> outputFilePathList){
        taskSituation=TaskSituation.done;
        this.executorTaskTrackerId=executorTaskTrackerId;
        this.outputFilePathList=outputFilePathList;
        this.executorTaskTrackerIpIPaddr =executorTaskTrackerIp;
    }
    public synchronized ArrayList<String> getOutputFilePaths(){
        if(taskSituation!=TaskSituation.done){
            throw new IllegalStateException("任务未完成");
        }
        return outputFilePathList;
    }
    public synchronized UUID getExecutorTaskTrackerId(){
        return executorTaskTrackerId;
    }
}