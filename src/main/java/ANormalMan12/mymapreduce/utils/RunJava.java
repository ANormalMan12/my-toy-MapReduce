package ANormalMan12.mymapreduce.utils;

import java.util.ArrayList;
import java.util.List;

public class RunJava {
    final static String JAVA_PATH="/home/wwy/.jdks/ibm-java-x86_64-80/bin/java";
    public  static List<String> getRunJavaArgList(List<String> argList) {
        List<String> retList=new ArrayList<>();
        retList.add(JAVA_PATH);
        retList.add("-cp");
        for(String arg:argList){
            retList.add(arg);
        }
        return retList;
    }
}
