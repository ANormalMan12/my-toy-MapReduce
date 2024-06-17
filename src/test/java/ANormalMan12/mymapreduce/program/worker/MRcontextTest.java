package ANormalMan12.mymapreduce.program.worker;

import junit.framework.TestCase;

public class MRcontextTest extends TestCase {

    public void testWrite() {
        MRcontext<String,Integer> stringIntegerMRcontext=new MRcontext<>(3);
        String[] strs={"hello","hello","hello","goodbye","goodbye","6"};
        for(String x:strs){
            stringIntegerMRcontext.write(x,1);
        }
    }
}