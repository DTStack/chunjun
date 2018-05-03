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

package com.dtstack.flinkx.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;

/**
 * The utility class of HBase
 *
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HbaseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseHelper.class);

    public static org.apache.hadoop.hbase.client.Connection getHbaseConnection(Map<String,String> hbaseConfigMap) {
        org.apache.hadoop.conf.Configuration hConfiguration = HBaseConfiguration.create();
        try {
            Validate.isTrue(hbaseConfigMap != null && hbaseConfigMap.size() !=0, "hbaseConfig不能为空Map结构!");
            for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet()) {
                hConfiguration.set(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        org.apache.hadoop.hbase.client.Connection hConnection = null;
        try {
            hConnection = ConnectionFactory.createConnection(hConfiguration);

        } catch (Exception e) {
            HbaseHelper.closeConnection(hConnection);
            throw new RuntimeException(e);
        }
        return hConnection;
    }

    public static RegionLocator getRegionLocator(Connection hConnection, String userTable){
        TableName hTableName = TableName.valueOf(userTable);
        org.apache.hadoop.hbase.client.Admin admin = null;
        RegionLocator regionLocator = null;
        try {
            admin = hConnection.getAdmin();
            HbaseHelper.checkHbaseTable(admin,hTableName);
            regionLocator = hConnection.getRegionLocator(hTableName);
        } catch (Exception e) {
            HbaseHelper.closeRegionLocator(regionLocator);
            HbaseHelper.closeAdmin(admin);
            HbaseHelper.closeConnection(hConnection);
            throw new RuntimeException(e);
        }
        return regionLocator;

    }

    public static byte[] convertRowkey(String rowkey, boolean isBinaryRowkey) {
        if(StringUtils.isBlank(rowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            return HbaseHelper.stringToBytes(rowkey, isBinaryRowkey);
        }
    }

    private static byte[] stringToBytes(String rowkey, boolean isBinaryRowkey) {
        if (isBinaryRowkey) {
            return Bytes.toBytesBinary(rowkey);
        } else {
            return Bytes.toBytes(rowkey);
        }
    }


    public static void closeConnection(Connection hConnection){
        try {
            if(null != hConnection) {
                hConnection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void closeAdmin(Admin admin){
        try {
            if(null != admin) {
                admin.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void closeRegionLocator(RegionLocator regionLocator){
        try {
            if(null != regionLocator) {
                regionLocator.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static  void checkHbaseTable(Admin admin,  TableName table) throws IOException {
        if(!admin.tableExists(table)){
            throw new IllegalArgumentException("hbase table " + table + " does not exist.");
        }
        if(!admin.isTableAvailable(table)){
            throw new RuntimeException("hbase table " + table + " is not available.");
        }
        if(admin.isTableDisabled(table)){
            throw new RuntimeException("hbase table " + table + " is disabled");
        }
    }

    public static void closeBufferedMutator(BufferedMutator bufferedMutator){
        try {
            if(null != bufferedMutator){
                bufferedMutator.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
