package ANormalMan12.mymapreduce.program.worker;

import java.io.Serializable;

public class Pair<K extends Comparable<K>,V extends Comparable<V>>implements Comparable<Pair<K,V>> , Serializable {
    K key;
    V val;
    public Pair(K key, V value){
        this.key=key;
        this.val=value;
    }

    public K getKey() {
        return key;
    }
    public V getVal(){
        return val;
    }
    @Override
    public int compareTo(Pair<K,V> other) {
        // 按照key属性升序排序
        return this.key.compareTo(other.key);
    }

    @Override
    public String toString() {
        return "Pair{" +
                "key=" + key +
                "val=" + val +
                '}';
    }

    @Override
    public int hashCode() {
        return key != null ? key.hashCode() : 0;
    }
}
