package ANormalMan12.mymapreduceApp.WordCount;

import ANormalMan12.mymapreduce.program.worker.MRcontext;
import ANormalMan12.mymapreduce.program.worker.Reducer;

import java.util.Iterator;

public class WordCountReducer extends Reducer<String, Integer, String, Integer> {
    @Override
    public void reduce(String key, Iterator<Integer> valueIterator, MRcontext<String, Integer> context) {
        int sum = 0;
        while (valueIterator.hasNext()) {
            sum += valueIterator.next();
        }
        context.write(key, sum);
    }
}
