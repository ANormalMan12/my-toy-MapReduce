package ANormalMan12.mymapreduce.tasktracker;

import junit.framework.TestCase;

public class WorkerTest extends TestCase {

    public void testMain() {
        String[] args=new String[3];
        args[0]="C:\\Users\\wangw\\IdeaProjects\\wwyMapReduce\\out\\artifacts\\wwyMapReduceOutput\\wwyMapReduce.jar";
        args[1]="ANormalMan12.mymapreduce.client.WordCount.WordCountReducer";
        args[2]="1";
        Worker.main(args);
    }
}