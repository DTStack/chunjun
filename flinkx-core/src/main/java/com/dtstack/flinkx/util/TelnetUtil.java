package com.dtstack.flinkx.util;

import org.apache.commons.net.telnet.TelnetClient;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TelnetUtil {

    private static Pattern JDBC_PATTERN = Pattern.compile("(?<host>[^:@/]+):(?<port>\\d+).*");
    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";

    public static void telnet(String ip,int port) {
        TelnetClient client = null;
        try{
            client = new TelnetClient();
            client.setConnectTimeout(3000);
            client.connect(ip,port);
        } catch (Exception e){
            throw new RuntimeException("Unable connect to : " + ip + ":" + port);
        } finally {
            try {
                if (client != null){
                    client.disconnect();
                }
            } catch (Exception ignore){
            }
        }
    }

    public static void telnet(String url) {
        if (url == null || url.trim().length() == 0){
            throw new IllegalArgumentException("url can not be null");
        }

        String host = null;
        int port = 0;

        Matcher matcher = JDBC_PATTERN.matcher(url);
        if (matcher.find()){
            host = matcher.group(HOST_KEY);
            port = Integer.parseInt(matcher.group(PORT_KEY));
        }

        if (host == null || port == 0){
            throw new IllegalArgumentException("The url format is incorrect");
        }

        System.out.println("host:" + host);
        System.out.println("port:" + port);

        telnet(host,port);
    }
}
