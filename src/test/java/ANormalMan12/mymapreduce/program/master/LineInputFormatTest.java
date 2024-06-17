package ANormalMan12.mymapreduce.program.master;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LineInputFormatTest extends TestCase {

    public void testGetSplitList() throws IOException {
        String testFilePath="src/test/resources/word_count_result.txt";
        List<FileSplitInfo> fileSplitInfoList = LineInputFormat.getSplitList(testFilePath,2);
        for(FileSplitInfo fileSplitInfo : fileSplitInfoList){
            System.out.println(fileSplitInfo);
            byte[] byteBuffer =LineInputFormat.getByteArrayBySplitInfoUtf8(testFilePath, fileSplitInfo);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteBuffer);
            InputStreamReader inputStream = new InputStreamReader(byteArrayInputStream, StandardCharsets.UTF_8);
            int data;
            while ((data = inputStream.read()) !=-1) {
                System.out.print((char)  data);
            }
        }
    }
}