/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadataphoenix5.util;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

import java.io.IOException;
import java.io.StringReader;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.AUTHENTICATION_TYPE;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HADOOP_SECURITY_AUTHENTICATION;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_MASTER_KERBEROS_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_SECURITY_AUTHENTICATION;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_SECURITY_AUTHORIZATION;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.KEYTAB_FILE;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.KEY_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.PHOENIX_QUERYSERVER_KERBEROS_PRINCIPAL;

public class Phoenix5Util {

    /**
     * 通过指定类加载器获取helper
     * @param parentClassLoader 类加载器
     * @return helper实现类
     * @throws IOException io异常
     * @throws CompileException 编译异常
     */
    public static IPhoenix5Helper getHelper(ClassLoader parentClassLoader) throws IOException, CompileException {
        ClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setParentClassLoader(parentClassLoader);
        cbe.setDefaultImports("com.dtstack.flinkx.util.ClassUtil",
                "com.dtstack.flinkx.util.TelnetUtil",
                "org.apache.commons.lang3.StringUtils",
                "org.apache.commons.lang3.tuple.Pair",
                "org.apache.flink.types.Row",
                "org.apache.hadoop.hbase.NoTagsKeyValue",
                "org.apache.hadoop.hbase.client.Result",
                "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                "org.apache.phoenix.compile.RowProjector",
                "org.apache.phoenix.compile.StatementContext",
                "org.apache.phoenix.jdbc.PhoenixEmbeddedDriver",
                "org.apache.phoenix.jdbc.PhoenixPreparedStatement",
                "org.apache.phoenix.jdbc.PhoenixResultSet",
                "org.apache.phoenix.query.KeyRange",
                "org.apache.phoenix.schema.tuple.ResultTuple",
                "org.apache.phoenix.schema.types.PBoolean",
                "org.apache.phoenix.schema.types.PChar",
                "org.apache.phoenix.schema.types.PDataType",
                "org.apache.phoenix.schema.types.PDate",
                "org.apache.phoenix.schema.types.PDecimal",
                "org.apache.phoenix.schema.types.PDouble",
                "org.apache.phoenix.schema.types.PFloat",
                "org.apache.phoenix.schema.types.PInteger",
                "org.apache.phoenix.schema.types.PLong",
                "org.apache.phoenix.schema.types.PSmallint",
                "org.apache.phoenix.schema.types.PTime",
                "org.apache.phoenix.schema.types.PTimestamp",
                "org.apache.phoenix.schema.types.PTinyint",
                "org.apache.phoenix.schema.types.PUnsignedDate",
                "org.apache.phoenix.schema.types.PUnsignedDouble",
                "org.apache.phoenix.schema.types.PUnsignedFloat",
                "org.apache.phoenix.schema.types.PUnsignedInt",
                "org.apache.phoenix.schema.types.PUnsignedLong",
                "org.apache.phoenix.schema.types.PUnsignedSmallint",
                "org.apache.phoenix.schema.types.PUnsignedTime",
                "org.apache.phoenix.schema.types.PUnsignedTimestamp",
                "org.apache.phoenix.schema.types.PUnsignedTinyint",
                "org.apache.phoenix.schema.types.PVarchar",
                "java.lang.reflect.Field",
                "java.sql.Connection",
                "java.sql.DriverManager",
                "java.sql.PreparedStatement",
                "java.sql.ResultSet",
                "java.sql.SQLException",
                "java.util.ArrayList",
                "java.util.Collections",
                "java.util.HashMap",
                "java.util.List",
                "java.util.Map",
                "java.util.NavigableSet",
                "java.util.Properties");
        cbe.setImplementedInterfaces(new Class[]{IPhoenix5Helper.class});
        StringReader sr = new StringReader(IPhoenix5Helper.CLASS_STR);
        return (IPhoenix5Helper) cbe.createInstance(sr);
    }

    /**
     * ugi认证下获取连接
     * @param hbaseConfigMap
     * @param properties
     * @param url
     * @param helper
     * @return
     */
    public static Connection getConnectionWithKerberos(Map<String, Object> hbaseConfigMap, Properties properties, String url, IPhoenix5Helper helper) {
        try {
            UserGroupInformation ugi = getUgi(hbaseConfigMap);
            return ugi.doAs((PrivilegedAction<Connection>) () -> {
                try {
                    return helper.getConn(url, properties);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Login kerberos error", e);
        }
    }



    /**
     * 设定phoenix认证所需要的kerberos参数
     * @param p
     * @param hbaseConfigMap
     * @return
     */
    public static void  setKerberosParams(Properties p, Map<String, Object> hbaseConfigMap) {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hbaseConfigMap);
        keytabFileName = KerberosUtil.loadFile(hbaseConfigMap, keytabFileName);
        String principal = KerberosUtil.getPrincipal(hbaseConfigMap, keytabFileName);
        KerberosUtil.loadKrb5Conf(hbaseConfigMap);
        KerberosUtil.refreshConfig();

        hbaseConfigMap.putIfAbsent(HBASE_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);
        hbaseConfigMap.putIfAbsent(HBASE_SECURITY_AUTHORIZATION, AUTHENTICATION_TYPE);
        hbaseConfigMap.putIfAbsent(HADOOP_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);
        hbaseConfigMap.putIfAbsent(PHOENIX_QUERYSERVER_KERBEROS_PRINCIPAL, principal);
        hbaseConfigMap.putIfAbsent(KEY_PRINCIPAL,principal);
        hbaseConfigMap.putIfAbsent(KEYTAB_FILE,keytabFileName);

        p.setProperty(HBASE_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);
        p.setProperty(HBASE_SECURITY_AUTHORIZATION, AUTHENTICATION_TYPE);
        p.setProperty(HADOOP_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);
        p.setProperty(HBASE_MASTER_KERBEROS_PRINCIPAL, MapUtils.getString(hbaseConfigMap, HBASE_MASTER_KERBEROS_PRINCIPAL));
        p.setProperty(HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, MapUtils.getString(hbaseConfigMap, HBASE_REGIONSERVER_KERBEROS_PRINCIPAL));
    }


    /**
     * 获取ugi信息
     * @param hbaseConfigMap
     * @return
     * @throws IOException
     */
    public static UserGroupInformation getUgi(Map<String, Object> hbaseConfigMap) throws IOException {
        Configuration conf = FileSystemUtil.getConfiguration(hbaseConfigMap, null);
        String principal = MapUtils.getString(hbaseConfigMap, KEY_PRINCIPAL);
        String keytabFileName = MapUtils.getString(hbaseConfigMap, KEYTAB_FILE);
        return KerberosUtil.loginAndReturnUgi(conf, principal, keytabFileName);
    }
}
