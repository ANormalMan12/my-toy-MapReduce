package ANormalMan12.mymapreduce.network.pastUtils;

import ANormalMan12.mymapreduce.utils.configuration.LocalDataManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

public class TcpFileServer implements  Runnable {

    private static final int PORT = 11300;
    private static final Path DIRECTORY = LocalDataManager.TMP_DIR_PATH;

    @Override
    public void run(){
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("TCP File Transport Service start on" + PORT);
            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
                     DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream())
                ) {
                    // 文件路径 起始字节 结束字节
                    String filePath = DIRECTORY + dataInputStream.readUTF();
                    long startByte = dataInputStream.readLong();
                    long endByte = dataInputStream.readLong();

                    File file = new File(filePath);
                    if (!file.exists() || file.isDirectory()) {
                        dataOutputStream.writeInt(404); // 文件不存在
                        continue;
                    }

                    dataOutputStream.writeInt(200); // OK
                    try (FileInputStream fileInputStream = new FileInputStream(file)) {
                        fileInputStream.skip(startByte);
                        byte[] buffer = new byte[8192]; // 8KB buffer
                        long bytesRemaining = endByte - startByte + 1;
                        int bytesRead;
                        while (bytesRemaining > 0 && (bytesRead = fileInputStream.read(buffer, 0, (int) Math.min(buffer.length, bytesRemaining))) != -1) {
                            dataOutputStream.write(buffer, 0, bytesRead);
                            bytesRemaining -= bytesRead;
                        }
                    }
                } catch (IOException e) {
                    System.out.println("未能与连入客户端建立连接");
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.out.println("Server 打开失败");
            e.printStackTrace();
        }
    }
}
