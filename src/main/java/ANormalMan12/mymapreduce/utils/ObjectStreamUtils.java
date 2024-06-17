package ANormalMan12.mymapreduce.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ObjectStreamUtils<T extends Serializable> {

    // 将对象写入字节流
    public byte[] writeObjectToByteStream(Iterator<T> objectIterator) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            while(objectIterator.hasNext()){
                objectOutputStream.writeObject(objectIterator.next());
            }
        }
        return byteArrayOutputStream.toByteArray();
    }
    // 从字节流中读取对象
    public List<T> readObjectFromByteStream(byte[] byteStream) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteStream);
        List<T> arrayList= new ArrayList<>();
        try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            while (byteArrayInputStream.available() > 0) {
                arrayList.add ((T) objectInputStream.readObject());
            }
        }
        return arrayList;
    }
}