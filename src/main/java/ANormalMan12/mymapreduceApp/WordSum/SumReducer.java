package ANormalMan12.mymapreduceApp.WordSum;

import ANormalMan12.mymapreduce.program.worker.MRcontext;
import ANormalMan12.mymapreduce.program.worker.Reducer;

import java.util.Iterator;

public class SumReducer extends Reducer<Integer,Integer,Integer,Integer> {
    @Override
    public void reduce(Integer key, Iterator<Integer> valueIterator, MRcontext<Integer, Integer> context) {
        int appearSum=0;
        while (valueIterator.hasNext()){
            int val=valueIterator.next();
            appearSum+=val;
        }
        context.write(key,appearSum);
    }
}
