package ANormalMan12.mymapreduceApp.WordCount;

import ANormalMan12.mymapreduce.program.worker.MRcontext;
import ANormalMan12.mymapreduce.program.worker.Mapper;

public class WordCountMapper extends Mapper<String, Integer> {
    @Override
    public void map(Integer key, String value, MRcontext<String, Integer> context) {
        for(String x:value.split(" ")){
            if (x.equals("")) {
                continue;
            }
            context.write(x,1);
        }
    }
}
