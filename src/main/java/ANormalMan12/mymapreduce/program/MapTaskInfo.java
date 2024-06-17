package ANormalMan12.mymapreduce.program;

import ANormalMan12.mymapreduce.program.master.GlobalFileSplitInfo;
import ANormalMan12.mymapreduce.program.master.TaskInfo;

public class MapTaskInfo extends TaskInfo {
    GlobalFileSplitInfo inputGLobalFileSplit;//该Map任务对应的文件分片的位置。
    public MapTaskInfo(int index,GlobalFileSplitInfo inputGlobalFileSplit){
        super(index);
        this.inputGLobalFileSplit=inputGlobalFileSplit;
    }

    public GlobalFileSplitInfo getInputGLobalFileSplit() {
        return inputGLobalFileSplit;
    }
}
