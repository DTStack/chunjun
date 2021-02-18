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

package com.dtstack.flinkx.metadataphoenix5.inputformat;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadataphoenix5.entity.Phoenix5ColumnEntity;
import com.dtstack.flinkx.metadataphoenix5.entity.Phoenix5TableEntity;
import com.dtstack.flinkx.metadataphoenix5.util.IPhoenix5Helper;
import com.dtstack.flinkx.metadataphoenix5.util.Phoenix5Util;
import com.dtstack.flinkx.util.ZkHelper;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;
import com.dtstack.metadata.rdb.inputformat.MetadatardbInputFormat;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.commons.compiler.CompileException;
import sun.misc.URLClassPath;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.HBASE_MASTER_KERBEROS_PRINCIPAL;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.KEY_DEFAULT;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.KEY_FALSE;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.KEY_TRUE;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.RESULT_SET_TABLE_NAME;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.SQL_COLUMN;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.SQL_DEFAULT_COLUMN;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.SQL_DEFAULT_TABLE_NAME;
import static com.dtstack.flinkx.metadataphoenix5.util.PhoenixMetadataCons.SQL_TABLE_NAME;
import static com.dtstack.flinkx.util.ZkHelper.APPEND_PATH;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_CONN_PASSWORD;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_USER;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_COLUMN_NAME;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_SET_ORDINAL_POSITION;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_TYPE_NAME;

/**
 * @author kunni@Dtstack.com
 */

public class Metadataphoenix5InputFormat extends MetadatardbInputFormat {

    /**表和创建时间集合*/
    private Map<String, Long> createTimeMap;

    /**phoenix url固定前缀*/
    public static final String JDBC_PHOENIX_PREFIX = "jdbc:phoenix:";

    /**默认schema名称*/
    public static final String DEFAULT_SCHEMA = "default";

    /**phoenix 依赖的zookeeper集群信息*/
    protected ZooKeeper zooKeeper;

    /**数据源配置参数，kerberos信息等*/
    protected Map<String, Object> hadoopConfig;

    /**phoenix在zookeeper上表空间路径*/
    protected String path;

    /**phoenix在zookeeper上znode*/
    protected String zooKeeperPath;


    @Override
    protected void doOpenInternal() {
        super.doOpenInternal();
        if (createTimeMap == null) {
            String hosts = connectionInfo.getJdbcUrl().substring(JDBC_PHOENIX_PREFIX.length());
            createTimeMap = queryCreateTimeMap(hosts);
        }
    }

    @Override
    public MetadatardbEntity createMetadatardbEntity() throws Exception {
        String tableName = (String) currentObject;
        MetadatardbEntity metadataPhoenix5Entity = new MetadatardbEntity();
        metadataPhoenix5Entity.setTableProperties(queryTableProp(tableName));
        metadataPhoenix5Entity.setColumns(queryColumn(tableName));
        return metadataPhoenix5Entity;
    }


    public List<Object> showTables() {
        String sql;
        if (StringUtils.isBlank(currentDatabase) || StringUtils.endsWithIgnoreCase(currentDatabase, KEY_DEFAULT)) {
            sql = SQL_DEFAULT_TABLE_NAME;
        } else {
            sql = String.format(SQL_TABLE_NAME, currentDatabase);
        }
        List<Object> table = new LinkedList<>();
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                table.add(resultSet.getString(RESULT_SET_TABLE_NAME));
            }
        } catch (SQLException e) {
            LOG.error("query table lists failed, {}", ExceptionUtil.getErrorMessage(e));
        }
        return table;
    }

    /**
     * 获取表级别的元数据信息
     *
     * @param tableName 表名
     * @return 表的元数据
     */
    public Phoenix5TableEntity queryTableProp(String tableName) {
        Phoenix5TableEntity phoenix5TableEntity = new Phoenix5TableEntity();
        if (StringUtils.isEmpty(currentDatabase) || StringUtils.endsWithIgnoreCase(currentDatabase, KEY_DEFAULT)) {
            phoenix5TableEntity.setCreateTime(String.valueOf(createTimeMap.get(tableName)));
        } else {
            phoenix5TableEntity.setCreateTime(String.valueOf(createTimeMap.get(currentDatabase + ConstantValue.POINT_SYMBOL + tableName)));
        }
        phoenix5TableEntity.setNameSpace(currentDatabase);
        phoenix5TableEntity.setTableName(tableName);
        return phoenix5TableEntity;
    }

    /**
     * 获取列级别的元数据信息
     *
     * @param tableName 表名
     * @return 列的元数据信息
     */
    public List<ColumnEntity> queryColumn(String tableName) {
        List<ColumnEntity> columns = new LinkedList<>();
        String sql;
        if (isDefaultSchema()) {
            sql = String.format(SQL_DEFAULT_COLUMN, tableName);
        } else {
            sql = String.format(SQL_COLUMN, currentDatabase, tableName);
        }
        Map<String, String> familyMap = new HashMap<>(16);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                familyMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            LOG.error("query column information failed, {}", ExceptionUtil.getErrorMessage(e));
        }
        //default schema需要特殊处理
        String originSchema = currentDatabase;
        if (DEFAULT_SCHEMA.equalsIgnoreCase(originSchema)) {
            originSchema = null;
        }
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, originSchema, tableName, null)) {
            while (resultSet.next()) {
                Phoenix5ColumnEntity phoenix5ColumnEntity = new Phoenix5ColumnEntity();
                int index = resultSet.getInt(RESULT_SET_ORDINAL_POSITION);
                String family = familyMap.get(index);
                if (StringUtils.isBlank(family)) {
                    phoenix5ColumnEntity.setIsPrimaryKey(KEY_TRUE);
                    phoenix5ColumnEntity.setName(resultSet.getString(RESULT_COLUMN_NAME));
                } else {
                    phoenix5ColumnEntity.setIsPrimaryKey(KEY_FALSE);
                    phoenix5ColumnEntity.setName(family + ConstantValue.COLON_SYMBOL + resultSet.getString(RESULT_COLUMN_NAME));
                }
                phoenix5ColumnEntity.setType(resultSet.getString(RESULT_TYPE_NAME));
                phoenix5ColumnEntity.setIndex(index);
                columns.add(phoenix5ColumnEntity);
            }
        } catch (SQLException e) {
            LOG.error("failed to get column information, {} ", ExceptionUtil.getErrorMessage(e));
        }
        return columns;
    }

    /**
     * 查询表的创建时间
     * 如果zookeeper没有权限访问，返回空map
     *
     * @param hosts zookeeper地址
     * @return 表名与创建时间的映射
     */
    protected Map<String, Long> queryCreateTimeMap(String hosts) {
        Map<String, Long> createTimeMap = new HashMap<>(16);
        try {
            zooKeeper = ZkHelper.createZkClient(hosts, ZkHelper.DEFAULT_TIMEOUT);
            List<String> tables = ZkHelper.getChildren(zooKeeper, path);
            if (tables != null) {
                for (String table : tables) {
                    createTimeMap.put(table, ZkHelper.getCreateTime(zooKeeper, path + ConstantValue.SINGLE_SLASH_SYMBOL + table));
                }
            }
        } catch (Exception e) {
            LOG.error("query createTime map failed, error {}", ExceptionUtil.getErrorMessage(e));
        } finally {
            ZkHelper.closeZooKeeper(zooKeeper);
        }
        return createTimeMap;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Field declaredField = ReflectionUtils.getDeclaredField(getClass().getClassLoader(), "ucp");
        assert declaredField != null;
        declaredField.setAccessible(true);
        URLClassPath urlClassPath;
        try {
            urlClassPath = (URLClassPath) declaredField.get(getClass().getClassLoader());
        } catch (IllegalAccessException e) {
            String message = String.format("cannot get urlClassPath from current classLoader, classLoader = %s, e = %s", getClass().getClassLoader(), ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
        declaredField.setAccessible(false);

        List<URL> needJar = Lists.newArrayList();
        for (URL url : urlClassPath.getURLs()) {
            String urlFileName = FilenameUtils.getName(url.getPath());
            if (urlFileName.startsWith("flinkx-metadata-phoenix5-reader")) {
                needJar.add(url);
                break;
            }
        }

        ClassLoader parentClassLoader = getClass().getClassLoader();
        List<String> list = new LinkedList<>();
        list.add("org.apache.flink");
        list.add("com.dtstack.flinkx");

        URLClassLoader childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, list.toArray(new String[0]));
        Properties properties = new Properties();
        ClassUtil.forName(connectionInfo.getDriver(), childFirstClassLoader);
        if (StringUtils.isNotEmpty(connectionInfo.getUsername())) {
            properties.setProperty(KEY_USER, connectionInfo.getUsername());
        }
        if (StringUtils.isNotEmpty(connectionInfo.getPassword())) {
            properties.setProperty(KEY_CONN_PASSWORD, connectionInfo.getPassword());
        }
        String jdbcUrl = connectionInfo.getJdbcUrl() + ConstantValue.COLON_SYMBOL + zooKeeperPath;
        try {
            IPhoenix5Helper helper = Phoenix5Util.getHelper(childFirstClassLoader);
            if (StringUtils.isNotEmpty(MapUtils.getString(hadoopConfig, HBASE_MASTER_KERBEROS_PRINCIPAL))) {
                Phoenix5Util.setKerberosParams(properties, hadoopConfig);
                return Phoenix5Util.getConnectionWithKerberos(hadoopConfig, properties, jdbcUrl, helper);
            }
            return helper.getConn(jdbcUrl, properties);
        } catch (IOException | CompileException e) {
            String message = String.format("cannot get phoenix connection, dbUrl = %s, properties = %s, e = %s", connectionInfo.getJdbcUrl(), GsonUtil.GSON.toJson(properties), ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }

    /**
     * phoenix 默认schema为空，在平台层设置值为default
     *
     * @return 是否为默认schema
     */
    public boolean isDefaultSchema() {
        return StringUtils.isBlank(currentDatabase) || StringUtils.endsWithIgnoreCase(currentDatabase, KEY_DEFAULT);
    }

    /**
     * 传入为hbase在zookeeper中的路径，增加/table表示table所在路径
     *
     * @param path hbase路径
     */
    public void setPath(String path) {
        this.zooKeeperPath = path;
        this.path = path + APPEND_PATH;
    }


    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }
}
