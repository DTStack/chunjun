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


package com.dtstack.flinkx.hive.test;

import com.dtstack.flinkx.hive.util.HiveDbUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/29
 */
public class DBUtilTest {

    public static void main(String[] args) throws Exception{
        Map<String, Object> sftpConf = new HashMap<>();
        sftpConf.put("host", "172.16.10.79");
        sftpConf.put("port", "22");
        sftpConf.put("username", "root");
        sftpConf.put("password", "abc123");

        Map<String, Object> hiveConf = new HashMap<>();
        hiveConf.put("hive.server2.authentication.kerberos.principal", "hive/cdh02@HADOOP.COM");
        hiveConf.put("hive.server2.authentication.kerberos.keytab", "D:\\cdh_cluster\\hive.keytab");
        hiveConf.put("java.security.krb5.conf", "D:\\cdh_cluster\\krb5.conf");
        hiveConf.put("useLocalFile", "true");
        hiveConf.put("sftpConf", sftpConf);
        hiveConf.put("remoteDir", "/home/sftp/keytab/jiangbo");

        HiveDbUtil.ConnectionInfo connectionInfo = new HiveDbUtil.ConnectionInfo();
        connectionInfo.setJdbcUrl("jdbc:hive2://172.16.10.75:10000/default;principal=hive/cdh02@HADOOP.COM");
        connectionInfo.setUsername("");
        connectionInfo.setPassword("");
        connectionInfo.setHiveConf(hiveConf);

        Connection connection = HiveDbUtil.getConnection(connectionInfo);
        ResultSet rs = connection.createStatement().executeQuery("show tables");
        while (rs.next()) {
            System.out.println(rs.getObject(2));
        }

        connection.close();
    }
}
