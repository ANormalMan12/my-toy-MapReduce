package ANormalMan12.mymapreduce.program.worker;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class LineLocalReader {
    public static Iterator<Pair<Integer, String>> readFile(String localInputPath) throws FileNotFoundException {
        List<Pair<Integer,String>> lines = new ArrayList<>();
        int localLineIndex=0;
        try (Scanner sc = new Scanner(new FileReader(localInputPath))) {
            while (sc.hasNextLine()) {
                lines.add(new Pair<>(localLineIndex,sc.nextLine()));
                localLineIndex++;
            }
        }
        return lines.iterator();
    }
}
