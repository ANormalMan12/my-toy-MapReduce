package ANormalMan12.mymapreduce.utils;

import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {
    public static String getTimeString(){
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss a");
        Date date = new Date();
        return sdf.format(date);
    }
}
