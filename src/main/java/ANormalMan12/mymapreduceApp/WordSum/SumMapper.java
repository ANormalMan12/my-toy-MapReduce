package ANormalMan12.mymapreduceApp.WordSum;

import ANormalMan12.mymapreduce.program.worker.MRcontext;
import ANormalMan12.mymapreduce.program.worker.Mapper;

public class SumMapper extends Mapper<Integer,Integer> {
    @Override
    public void map(Integer key, String value, MRcontext<Integer, Integer> context) {
        int sum=0;
        for(String x:value.split(" ")){
            if(x.equals(""))continue;
            sum+=Integer.valueOf(x);
        }
        context.write(sum,1);
    }
}
