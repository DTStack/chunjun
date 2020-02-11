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


package com.dtstack.flinkx.hbase.test;

import com.dtstack.flinkx.hbase.HbaseHelper;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/29
 */
public class HbaseHelperTest {

    public static void main(String[] args) throws Exception{
        Map<String, Object> sftpConf = new HashMap<>();
        sftpConf.put("host", "172.16.10.79");
        sftpConf.put("port", "22");
        sftpConf.put("username", "root");
        sftpConf.put("password", "abc123");

        Map<String, Object> hbaseConfig = new HashMap<>();
        hbaseConfig.put("hbase.security.authorization", "true");
        hbaseConfig.put("hbase.security.authentication", "kerberos");
        hbaseConfig.put("hbase.master.kerberos.principal", "hbase/cdh01@HADOOP.COM");
        hbaseConfig.put("hbase.master.keytab.file", "D:\\cdh_cluster\\cdh2\\hbase.keytab");
        hbaseConfig.put("hbase.regionserver.kerberos.principal", "hbase/cdh01@HADOOP.COM");
        hbaseConfig.put("hbase.regionserver.keytab.file", "D:\\cdh_cluster\\cdh2\\hbase.keytab");
        hbaseConfig.put("java.security.krb5.conf", "D:\\cdh_cluster\\cdh2\\krb5.conf");
        hbaseConfig.put("useLocalFile", "true");
//        hbaseConfig.put("sftpConf", sftpConf);
//        hbaseConfig.put("remoteDir", "/home/sftp/keytab/jiangbo");

//        hbaseConfig.put("hbase.zookeeper.quorum", "cdh01:2181,cdh02:2181,cdh03:2181");
        hbaseConfig.put("hbase.zookeeper.quorum", "172.16.10.201:2181");
        hbaseConfig.put("hbase.rpc.timeout", "60000");
        hbaseConfig.put("ipc.socket.timeout", "20000");
        hbaseConfig.put("hbase.client.retries.number", "3");
        hbaseConfig.put("hbase.client.pause", "100");
        hbaseConfig.put("zookeeper.recovery.retry", "3");

        Connection connection = HbaseHelper.getHbaseConnection(hbaseConfig);
        Table table = connection.getTable(TableName.valueOf("tb1"));

        ResultScanner rs = table.getScanner(new Scan());
        Result result = rs.next();
        if(result != null){
            System.out.println(result.getRow());
        }

        HbaseHelper.getRegionLocator(connection, "tb1");

        connection.close();
    }
}
