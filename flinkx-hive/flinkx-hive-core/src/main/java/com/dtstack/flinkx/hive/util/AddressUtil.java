/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.hive.util;

import com.google.common.collect.Lists;
import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by sishu.yss on 2018/3/1.
 */
public class AddressUtil {

    private static Logger logger = LoggerFactory.getLogger(AddressUtil.class);

    private static List<String> localAddrList = Lists.newArrayList("0.0.0.0", "127.0.0.1", "localhost");


    /**
     * 获取本地ip地址，有可能会有多个地址, 若有多个网卡则会搜集多个网卡的ip地址
     */
    public static Set<InetAddress> resolveLocalAddresses() {
        Set<InetAddress> addrs = new HashSet<InetAddress>();
        Enumeration<NetworkInterface> ns = null;
        try {
            ns = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            logger.error("",e);
        }
        while (ns != null && ns.hasMoreElements()) {
            NetworkInterface n = ns.nextElement();
            Enumeration<InetAddress> is = n.getInetAddresses();
            while (is.hasMoreElements()) {
                InetAddress i = is.nextElement();
                if (!i.isLoopbackAddress() && !i.isLinkLocalAddress() && !i.isMulticastAddress()
                        && !isSpecialIp(i.getHostAddress())) {addrs.add(i);}
            }
        }
        return addrs;
    }

    private static boolean isSpecialIp(String ip) {
        if (ip.contains(":")) {return true;}
        if (ip.startsWith("127.")) {return true;}
        if (ip.startsWith("169.254.")) {return true;}
        if (ip.equals("255.255.255.255")) {return true;}
        return false;
    }

    public static List<String> resolveLocalIps() {
        Set<InetAddress> addrs = resolveLocalAddresses();
        List<String> ret = Lists.newArrayList();
        for (InetAddress addr : addrs){
            String ar = addr.getHostAddress();
            if(!ret.contains(ar)){
                ret.add(ar);
            }
        }
        return ret;
    }

    public static String getOneIP(){
        List<String> ips =   resolveLocalIps();
        return ips.size()>0?ips.get(0):"0.0.0.0";
    }


    public static boolean telnet(String ip,int port){
        TelnetClient client = null;
        try{
            client = new TelnetClient();
            client.setConnectTimeout(3000);
            client.connect(ip,port);
            return true;
        }catch(Exception e){
            return false;
        } finally {
            try {
                if (client != null){
                    client.disconnect();
                }
            } catch (Exception e){
                logger.error("{}",e);
            }
        }
    }

    public static boolean ping(String ip){
        try{
            return InetAddress.getByName(ip).isReachable(3000);
        }catch(Exception e){
            return false;
        }
    }

    /**
     * 校验ip是否是'0.0.0.0', '127.0.0.1'
     * @param ip
     * @return
     */
    public static boolean checkAddrIsLocal(String ip){
        for(String localIp : localAddrList){
            if(localIp.equalsIgnoreCase(ip)){
                return true;
            }
        }

        return false;
    }

    /**
     * 检查服务是否相同：ip相同，端口相同
     */
    public static boolean checkServiceIsSame(String host1,int port1,String host2,int port2) throws Exception{
        InetAddress address1 = InetAddress.getByName(host1);
        InetAddress address2 = InetAddress.getByName(host2);

        return address1.getHostAddress().equals(address2.getHostAddress()) && port1 == port2;
    }

    public static void main(String[] args){
        System.out.print(getOneIP());
    }
}
