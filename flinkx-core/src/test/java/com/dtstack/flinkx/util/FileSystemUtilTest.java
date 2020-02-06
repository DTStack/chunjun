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


package com.dtstack.flinkx.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/28
 */
public class FileSystemUtilTest {

    public static void main(String[] args) throws Exception{

        Map<String, Object> sftpConf = new HashMap<>();
        sftpConf.put("host", "172.16.10.79");
        sftpConf.put("port", "22");
        sftpConf.put("username", "root");
        sftpConf.put("password", "abc123");


        Map<String, Object> hadoopConf = new HashMap<>();
        hadoopConf.put("dfs.namenode.rpc-address", "cdh01:9000");
        hadoopConf.put("hadoop.security.authorization", "true");
        hadoopConf.put("hadoop.security.authentication", "Kerberos");
        hadoopConf.put("dfs.namenode.kerberos.principal", "hdfs/_HOST@HADOOP.COM");
//        hadoopConf.put("dfs.namenode.keytab.file", "/opt/keytab/hdfs.keytab");
        hadoopConf.put("dfs.namenode.keytab.file", "D:\\cdh_cluster\\hdfs.keytab");
        hadoopConf.put("java.security.krb5.conf", "D:\\cdh_cluster\\krb5.conf");
        hadoopConf.put("useLocalFile", "false");
        hadoopConf.put("sftpConf", sftpConf);
        hadoopConf.put("remoteDir", "/home/sftp/keytab/jiangbo");

        FileSystem fs = FileSystemUtil.getFileSystem(hadoopConf, "hdfs://cdh01:9000", "sdddddddd","test");

        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
            System.out.println(status.getPath().getName());
        }

//        fs.create(new Path("/jiangbo"));

        fs.close();
    }
}
