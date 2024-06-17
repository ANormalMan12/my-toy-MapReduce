package ANormalMan12.mymapreduce.network.pastUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class netLocation {
    void getLocalAddress(){
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress()) { // 排除环回地址
                        System.out.println(networkInterface.getName() + " - IP地址: " + inetAddress.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            System.out.println("无法获取IP地址: " + e.getMessage());
        }
    }
}
