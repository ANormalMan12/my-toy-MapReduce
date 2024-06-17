package ANormalMan12.mymapreduce.program.master;

import java.io.Serializable;

public class FileSplitInfo  implements Serializable {
    static final int SPLIT_SIZE=1280000;
    long beginByte;
    long endByte;
    FileSplitInfo(long beginByte, long endByte){
        this.beginByte=beginByte;
        this.endByte=endByte;
    }

    @Override
    public String toString() {
        return "("+beginByte+","+endByte+")";
    }
}
