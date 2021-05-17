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
package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.oracle9.IOracle9Helper;
import com.dtstack.flinkx.oracle9.Oracle9DatabaseMeta;
import com.dtstack.flinkx.oracle9.OracleUtil;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:29
 */
public class Oracle9OutputFormat extends JdbcOutputFormat {


    protected static final int SECOND_WAIT = 30;

    private URLClassLoader childFirstClassLoader;
    private IOracle9Helper helper;

    //压缩文件名称
    private final String ZIP_NAME = "flinkx-oracle9writer.zip";
    //taskmanager本地路径
    private String currentPath;
    //解压后的jar包路径
    private String needLoadJarPath;
    //解压jar包临时路径
    private String unzipTempPath;
    //解压完成路径
    private String actionPath;


    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        try {
            actionBeforeWriteData();

            dbConn = getConnection();

            //默认关闭事务自动提交，手动控制事务
            dbConn.setAutoCommit(false);

            if (CollectionUtils.isEmpty(fullColumn)) {
                fullColumn = probeFullColumns(getTable(), dbConn);
            }

            if (!EWriteMode.INSERT.name().equalsIgnoreCase(mode)) {
                if (updateKey == null || updateKey.size() == 0) {
                    updateKey = probePrimaryKeys(getTable(), dbConn);
                }
            }

            if (fullColumnType == null) {
                fullColumnType = analyzeTable();
            }

            for (String col : column) {
                for (int i = 0; i < fullColumn.size(); i++) {
                    if (col.equalsIgnoreCase(fullColumn.get(i))) {
                        columnType.add(fullColumnType.get(i));
                        break;
                    }
                }
            }

            preparedStatement = prepareTemplates();
            readyCheckpoint = false;

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } finally {
            DbUtil.commit(dbConn);
        }
    }

    @Override
    protected Object getField(Row row, int index) {
        Object field = super.getField(row, index);
        String type = columnType.get(index);

        //oracle timestamp to oracle varchar or varchar2 or long field format
        if (!(field instanceof Timestamp)) {
            return field;
        }

        if (type.equalsIgnoreCase(ColumnType.VARCHAR.name()) || type.equalsIgnoreCase(ColumnType.VARCHAR2.name())) {
            SimpleDateFormat format = DateUtil.getDateTimeFormatter();
            field = format.format(field);
        }

        if (type.equalsIgnoreCase(ColumnType.LONG.name())) {
            field = ((Timestamp) field).getTime();
        }
        return field;
    }

    @Override
    protected List<String> probeFullColumns(String table, Connection dbConn) throws SQLException {
        String schema = null;

        String[] parts = table.split("\\.");
        if (parts.length == Oracle9DatabaseMeta.DB_TABLE_PART_SIZE) {
            schema = parts[0].toUpperCase();
            table = parts[1];
        }

        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, schema, table, null);
        while (rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }

    @Override
    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>(16);

        try (PreparedStatement ps = dbConn.prepareStatement(String.format(GET_INDEX_SQL, table));
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if (!map.containsKey(indexName)) {
                    map.put(indexName, new ArrayList<>());
                }
                map.get(indexName).add(rs.getString("COLUMN_NAME"));
            }

            Map<String, List<String>> retMap = new HashMap<>((map.size() << 2) / 3);
            for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                String k = entry.getKey();
                List<String> v = entry.getValue();
                if (v != null && v.size() != 0 && v.get(0) != null) {
                    retMap.put(k, v);
                }
            }
            return retMap;
        }
    }

    /**
     * 获取数据库连接
     *
     * @return Connection
     */
    public Connection getConnection() {
        List<URL> needJar = Lists.newArrayList();
        Set<URL> collect = new HashSet<>();
        for (String s1 : PluginUtil.getAllJarNames(new File(needLoadJarPath))) {
            try {
                collect.add(new URL("file:" + needLoadJarPath + File.separator + s1));
            } catch (MalformedURLException e) {
                throw new RuntimeException("get  [" + "file:" + needLoadJarPath + File.separator + s1 + "] failed", e);
            }
        }
        needJar.addAll(collect);
        LOG.info("need jars {} ", GsonUtil.GSON.toJson(needJar));

        ClassLoader parentClassLoader = getClass().getClassLoader();
        List<String> list = new LinkedList<>();
        list.add("org.apache.flink");
        list.add("com.dtstack.flinkx");

        childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, list.toArray(new String[0]));

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(childFirstClassLoader);

        ClassUtil.forName(driverName, childFirstClassLoader);

        try {
            helper = OracleUtil.getOracleHelper(childFirstClassLoader);
            return helper.getConnection(dbUrl, username, password);
        } catch (Exception e) {
            String message = String.format("can not get oracle connection , dbUrl = %s, e = %s", dbUrl, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    protected void actionBeforeWriteData() {
        String osName = System.getProperties().getProperty("os.name");
        if (osName.toLowerCase(Locale.ENGLISH).contains("windows")) {
            //window环境
            currentPath = Paths.get("").toAbsolutePath().toString();
        } else {
            //linux环境
            currentPath = SysUtil.getCurrentPath();
        }
        LOG.info("current path is {}", currentPath);


        File zipFile = new File(currentPath);
        zipFile = SysUtil.findFile(zipFile, ZIP_NAME);
        if (zipFile == null) {
            throw new RuntimeException("File " + zipFile.getAbsolutePath() + "  not exists,please sure upload this file");
        }

        needLoadJarPath = zipFile.getAbsolutePath().substring(0, zipFile.getAbsolutePath().lastIndexOf(".zip"));
        actionPath = needLoadJarPath + File.separator + "action";
        unzipTempPath = needLoadJarPath + File.separator + ".unzip";


        LOG.info("needLoadJarPath {}", needLoadJarPath);

        if (waitForActionFinishedBeforeWrite()) {
            //获取zip进行解压缩
            try {
                File unzipDirectory = new File(unzipTempPath);
                if (!unzipDirectory.mkdir()) {
                    throw new RuntimeException("create directory [ " + unzipTempPath + "] failed");
                }

                List<String> jars = SysUtil.unZip(zipFile.getAbsolutePath(), unzipTempPath);

                for (String jarPath : jars) {
                    File file = new File(jarPath);
                    file.renameTo(new File(needLoadJarPath + File.separator + file.getName()));
                }

                unzipDirectory.delete();

                File actionFile = new File(actionPath);
                if (!actionFile.mkdir()) {
                    throw new RuntimeException("create file [ " + actionFile.getAbsolutePath() + "] failed");
                }
            } catch (IOException e) {
                new File(needLoadJarPath).deleteOnExit();
                throw new RuntimeException(e);
            }

        }

    }


    /**
     * 创建解压目录 如果创建成功 则当前subtask进行解压 其余的channel进行等待
     *
     * @return
     */
    protected boolean waitForActionFinishedBeforeWrite() {
        boolean result = false;
        File unzipFile = new File(needLoadJarPath);
        File actionFile = new File(actionPath);
        int n = 0;
        //如果解压路径存在就不新建 否则代表其他的任务在新建中
        if (!unzipFile.exists()) {
            try {
                result = unzipFile.mkdir();
            } catch (Exception e) {
                if (!unzipFile.exists()) {
                    throw new RuntimeException("create directory" + needLoadJarPath + " failed", e);
                }
            }
        }
        //如果创建成功 就返回true 否则就等待其他channel完成新建
        if (result) {
            return result;
        }

        while (!actionFile.exists()) {
            if (n > SECOND_WAIT) {
                throw new RuntimeException("Wait action finished before write timeout");
            }
            SysUtil.sleep(3000);
            n++;
        }
        //如果等到其他任务创建成功 就返回false
        return result;
    }
}
