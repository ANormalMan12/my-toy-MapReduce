package ANormalMan12.mymapreduce.program.master;

import java.io.Serializable;

public class GlobalFileSplitInfo implements Serializable {
    public FileSplitInfo fileSplitInfo;
    public String filePath;
    public String ipAddress;
    public GlobalFileSplitInfo(String ipAddress, String filePath, FileSplitInfo fileSplitInfo){
        this.ipAddress=ipAddress;
        this.filePath=filePath;
        this.fileSplitInfo=fileSplitInfo;
    }

    @Override
    public String toString() {
        return ipAddress+":"+filePath+":"+fileSplitInfo;
    }
}
