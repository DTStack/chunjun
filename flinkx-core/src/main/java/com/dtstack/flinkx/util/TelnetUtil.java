package com.dtstack.flinkx.util;

import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TelnetUtil {

    protected static final Logger LOG = LoggerFactory.getLogger(TelnetUtil.class);

    private static Pattern JDBC_PATTERN = Pattern.compile("(?<host>[^:@/]+):(?<port>\\d+).*");
    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";

    public static void telnet(String ip,int port) {
        try {
            RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    TelnetClient client = null;
                    try{
                        client = new TelnetClient();
                        client.setConnectTimeout(3000);
                        client.connect(ip,port);
                        return true;
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
            }, 3,1000,false);
        } catch (Exception e) {
            LOG.warn("", e);
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
            //oracle高可用jdbc url此处获取不到IP端口，直接return。
            return;
        }

        System.out.println("host:" + host);
        System.out.println("port:" + port);

        telnet(host,port);
    }
}
