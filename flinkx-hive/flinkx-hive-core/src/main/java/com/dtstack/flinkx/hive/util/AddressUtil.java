/*
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

import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 * @author jiangbo
 * @date 2020/3/2
 */
public class AddressUtil {

    private static Logger logger = LoggerFactory.getLogger(AddressUtil.class);

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
                logger.error("{}", ExceptionUtil.getErrorMessage(e));
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
}
