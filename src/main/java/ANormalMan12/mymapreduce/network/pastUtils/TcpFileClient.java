package ANormalMan12.mymapreduce.network.pastUtils;

import java.io.*;
import java.net.Socket;

public class TcpFileClient {
    public static void downloadFile(String serverAddress, int port, String remoteFilePath, long startByte, long endByte, String localFilePath) throws IOException {
        try (
            Socket socket = new Socket(serverAddress, port);
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            FileOutputStream fileOutputStream = new FileOutputStream(localFilePath)
        ) {
            dataOutputStream.writeUTF(remoteFilePath);
            dataOutputStream.writeLong(startByte);
            dataOutputStream.writeLong(endByte);

            // 读取服务器响应
            int responseCode = dataInputStream.readInt();
            if (responseCode != 200) {
                System.err.println("Error: " + responseCode);
                return;
            }

            // 接收文件内容
            byte[] buffer = new byte[8192]; // 8KB buffer
            int bytesRead;
            while ((bytesRead = dataInputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
            System.out.println("File downloaded successfully.");
        }
    }

}
