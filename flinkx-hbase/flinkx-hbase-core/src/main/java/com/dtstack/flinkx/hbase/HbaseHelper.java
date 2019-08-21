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

import com.dtstack.flinkx.authenticate.KerberosUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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

    private final static String AUTHENTICATION_TYPE = "Kerberos";
    private final static String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    private final static String KEY_HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";
    private final static String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";
    private final static String KEY_HBASE_MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";
    private final static String KEY_HBASE_MASTER_KEYTAB_FILE = "hbase.master.keytab.file";
    private final static String KEY_HBASE_REGIONSERVER_KEYTAB_FILE = "hbase.regionserver.keytab.file";
    private final static String KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
    private final static String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private static List<String> KEYS_KERBEROS_REQUIRED = Arrays.asList(
            KEY_HBASE_SECURITY_AUTHENTICATION,
            KEY_HBASE_MASTER_KERBEROS_PRINCIPAL,
            KEY_HBASE_MASTER_KEYTAB_FILE,
            KEY_HBASE_REGIONSERVER_KEYTAB_FILE,
            KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL
    );

    public static org.apache.hadoop.hbase.client.Connection getHbaseConnection(Map<String,String> hbaseConfigMap) {
        Validate.isTrue(hbaseConfigMap != null && hbaseConfigMap.size() !=0, "hbaseConfig不能为空Map结构!");

        if(openKerberos(hbaseConfigMap)){
            login(hbaseConfigMap);
        }

        try {
            Configuration hConfiguration = getConfig(hbaseConfigMap);
            return ConnectionFactory.createConnection(hConfiguration);
        } catch (IOException e) {
            LOG.error("Get connection fail with config:{}", hbaseConfigMap);
            throw new RuntimeException(e);
        }
    }

    public static Configuration getConfig(Map<String,String> hbaseConfigMap){
        Configuration hConfiguration = HBaseConfiguration.create();
        for (Map.Entry<String, String> entry : hbaseConfigMap.entrySet()) {
            hConfiguration.set(entry.getKey(), entry.getValue());
        }

        return hConfiguration;
    }

    private static boolean openKerberos(Map<String,String> hbaseConfigMap){
        if(!MapUtils.getBooleanValue(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHORIZATION)){
            return false;
        }

        return AUTHENTICATION_TYPE.equalsIgnoreCase(MapUtils.getString(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHENTICATION));
    }

    private static void login(Map<String,String> hbaseConfigMap){
        for (String key : KEYS_KERBEROS_REQUIRED) {
            if(StringUtils.isEmpty(MapUtils.getString(hbaseConfigMap, key))){
                throw new IllegalArgumentException(String.format("Must provide [%s] when authentication is Kerberos", key));
            }
        }

        hbaseConfigMap.put(KEY_HADOOP_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);

        KerberosUtil.loadKeyTabFilesAndRepalceHost(hbaseConfigMap);

        String principal = MapUtils.getString(hbaseConfigMap, KEY_HBASE_MASTER_KERBEROS_PRINCIPAL);
        String path = MapUtils.getString(hbaseConfigMap, KEY_HBASE_MASTER_KEYTAB_FILE);
        String krb5Conf = MapUtils.getString(hbaseConfigMap, KEY_JAVA_SECURITY_KRB5_CONF);

        if(StringUtils.isNotEmpty(krb5Conf)){
            System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
        }

        UserGroupInformation.setConfiguration(getConfig(hbaseConfigMap));
        try {
            UserGroupInformation.loginUserFromKeytab(principal, path);
        } catch (IOException e){
            LOG.error("Login fail with config:{}", hbaseConfigMap);
            throw new RuntimeException("Login fail", e);
        }
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
