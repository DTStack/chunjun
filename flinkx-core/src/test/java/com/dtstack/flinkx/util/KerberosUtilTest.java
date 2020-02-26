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

import com.dtstack.flinkx.authenticate.KerberosUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2020/2/11
 */
public class KerberosUtilTest {

    @Test
    public void loadFileTest() {
        String filePath = "xiangqing.keytab";
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put("principalFile", "");
        kerberosConfig.put("remoteDir", "/home/admin/STREAM_653/hive_hbase_hdfs_kerberos");

        Map<String, Object> sftpConf = new HashMap<>();
        sftpConf.put("host", "172.16.8.193");
        sftpConf.put("port", "22");
        sftpConf.put("username", "root");
        sftpConf.put("password", "abc123");
        kerberosConfig.put("sftpConf", sftpConf);

        filePath = KerberosUtil.loadFile(kerberosConfig, filePath);
        System.out.println(filePath);
    }
}
