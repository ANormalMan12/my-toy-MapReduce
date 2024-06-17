package ANormalMan12.mymapreduce.program.worker;

import ANormalMan12.mymapreduce.utils.ObjectStreamUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MRcontext <
        K extends Comparable<K> & Serializable,
        V extends Comparable<V> & Serializable>{
    ArrayList<Pair<K,V>>[] arrayListBuckets;
    final int R;
    MRcontext(int R){
        this.R=R;
        arrayListBuckets=new ArrayList[R];
        for (int i = 0; i < R; i++) {
            arrayListBuckets[i] = new ArrayList<Pair<K, V>>();
        }
    }
    public void write(K key,V val){
        int hashIr=Math.floorMod(key.hashCode(), R);
        arrayListBuckets[hashIr].add(new Pair<>(key,val));
    }

    public ArrayList<byte[]> getRListsOfBytes() throws IOException {
        ObjectStreamUtils<Pair<K,V>> readWriter=new ObjectStreamUtils<>();
        ArrayList<byte[]> returnResult=new ArrayList<>();
        for(int i=0;i<R;++i){
            returnResult.add(
                    readWriter.writeObjectToByteStream(
                            arrayListBuckets[i].iterator()
                    )
            );
        }
        return returnResult;
    }
    public byte[] getBytesDataInUtf8() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < R; ++i) {
            for (Pair<K, V> pair : arrayListBuckets[i]) {
                sb.append("(").append(pair.key).append(", ").append(pair.val).append(")\n");
            }
        }
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
