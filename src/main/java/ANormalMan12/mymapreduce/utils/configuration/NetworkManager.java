package ANormalMan12.mymapreduce.utils.configuration;

public class NetworkManager {
    public static String getJobTrackerHostName(){
        return "192.168.56.101";
    }
    public static final int TASKTRACKER_REPEATED_MS_TIME=3000;
    public static final int JOBTRACKER_MISSED_MS_TIME=6000;
}
