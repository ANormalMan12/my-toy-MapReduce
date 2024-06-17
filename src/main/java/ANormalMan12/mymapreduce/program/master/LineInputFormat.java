package ANormalMan12.mymapreduce.program.master;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class LineInputFormat {
    public static List<FileSplitInfo> getSplitList(String localFilePath, int M) throws IOException{
        RandomAccessFile file = new RandomAccessFile(localFilePath,"r");
        long fileSize=file.length();
        long approximateSegmentSize=fileSize/M;
        long beginPosition=0;
        ArrayList<FileSplitInfo> splits=new ArrayList<>();
        for(int i=1;i<=M;++i){
            long endPosition;
            if(i==M) {
                endPosition=fileSize;
            }else{
                endPosition = i * approximateSegmentSize;
                endPosition = findFormerEnter(endPosition,file);
            }
            splits.add(new FileSplitInfo(beginPosition,endPosition));
            beginPosition=endPosition;
        }
        return splits;
    }
    public static byte[] getByteArrayBySplitInfoUtf8(String localFilePath, FileSplitInfo fileSplitInfo){
        long startByte = fileSplitInfo.beginByte;
        long endByte = fileSplitInfo.endByte;
        long length = endByte - startByte;
        byte[] buffer = new byte[(int) length];
        try (RandomAccessFile raf = new RandomAccessFile(localFilePath, "r")) {
            raf.seek(startByte);
            // 读取字节到缓冲区
            int bytesRead = raf.read(buffer, 0, (int) length);
            // 检查实际读取的字节数
            if (bytesRead ==-1){
                throw new IllegalArgumentException("非法的byte数");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;

    }
/*
    static long findFormerEnter(long bytePosition, RandomAccessFile file) throws IOException {
        for(; bytePosition>0;++bytePosition) {
            file.seek(bytePosition);
            byte byteContent = file.readByte();
            if (byteContent == '\n') {
                return bytePosition;
            }
        }
        return -1;
    }
    */
    static long findFormerEnter(long bytePosition, RandomAccessFile file) throws IOException {
        if (bytePosition < 0 || file == null) {
            return -1;
        }

        file.seek(bytePosition);
        while (bytePosition > 0) {
            file.seek(--bytePosition);
            byte byteContent = file.readByte();
            if (byteContent == '\n') {
                return bytePosition;
            }

            // 检查是否是UTF-8字符的续字节
            if ((byteContent & 0xC0) == 0x80) {
                // 继续往回读，直到找到一个不是续字节的字节
                while (bytePosition > 0) {
                    file.seek(--bytePosition);
                    byteContent = file.readByte();
                    if ((byteContent & 0xC0) != 0x80) {
                        file.seek(bytePosition); // 回到字符起始位置
                        break;
                    }
                }
            }
        }
        return -1;
    }

}
