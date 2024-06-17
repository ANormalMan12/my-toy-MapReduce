package ANormalMan12.mymapreduce.utils.configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.nio.file.Files;
import java.util.stream.Stream;

public class LocalDataManager {
    public static final Path TMP_DIR_PATH = Paths.get("/tmp/myMapReduceDir");
    public static final String INPUT_DIRNAME="input";
    public static final String MAP_RESULT_DIRNAME = "mapResults";
    public static final String REDUCE_RESULT_DIRNAME = "reduceResults";

    public static Path writeToLocalFile(String tmpSubDirName,String fileName,byte[] data){
        Path subDirPath = TMP_DIR_PATH.resolve(tmpSubDirName);
        try {
            if (!Files.exists(subDirPath)) {
                Files.createDirectories(subDirPath);
            }
            Path filePath = subDirPath.resolve(fileName);
            Files.write(filePath, data);
            return filePath;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Path copyFileWithRename(Path sourceFile, Path destinationFolder) throws IOException {
        String fileName = sourceFile.getFileName().toString();//获得文件名
        String fileExtension = "";
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex != -1) {
            fileExtension = fileName.substring(dotIndex);
            fileName = fileName.substring(0, dotIndex);
        }
        Path destinationFile = destinationFolder.resolve(fileName + "_copy" + fileExtension);
        Files.copy(sourceFile, destinationFile, StandardCopyOption.REPLACE_EXISTING);
        return destinationFile;
    }
    public static void clearTmpDir() throws IOException {
        deleteDirectory(TMP_DIR_PATH);
        createDirectoryIfNotExists(TMP_DIR_PATH); // 重新创建空目录
    }

    private static void deleteDirectory(Path path) {
        try (Stream<Path> paths = Files.walk(path)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(LocalDataManager::deletePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void deletePath(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void createDirectoryIfNotExists(Path path) throws  IOException{
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }
    public static String version="1.0.1";
}
