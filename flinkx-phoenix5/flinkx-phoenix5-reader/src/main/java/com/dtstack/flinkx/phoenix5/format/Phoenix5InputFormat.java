/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.phoenix5.format;

import com.dtstack.flinkx.phoenix5.Phoenix5DatabaseMeta;
import com.dtstack.flinkx.phoenix5.Phoenix5InputSplit;
import com.dtstack.flinkx.phoenix5.util.IPhoenix5Helper;
import com.dtstack.flinkx.phoenix5.util.PhoenixUtil;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.RangeSplitUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.query.QueryConstants;
import org.codehaus.commons.compiler.CompileException;
import sun.misc.URLClassPath;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Company: www.dtstack.com
 *
 * @author wuhui
 */
public class Phoenix5InputFormat extends JdbcInputFormat {

    //是否直接读取HBase的数据
    public boolean readFromHbase;
    //一次读取返回的Results数量,默认为HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING
    public int scanCacheSize;
    //限定了一个Result中所包含的列的数量，如果一行数据被请求的列的数量超出Batch限制，那么这行数据会被拆成多个Results
    //默认为-1，表示返回所有行
    public int scanBatchSize;

    public String sql;
    private transient Scan scan;
    private transient Table hTable;
    private transient Iterator<Pair<byte[], byte[]>> keyRangeIterator;
    private transient Iterator<Result> resultIterator;

    private transient URLClassLoader childFirstClassLoader;
    private transient IPhoenix5Helper helper;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        if(readFromHbase){
            LOG.warn("phoenix5reader config [readFromHbase] is true, FlinkX will read data from HBase directly!");
            Phoenix5DatabaseMeta metaData = (Phoenix5DatabaseMeta) this.databaseInterface;
            sql = metaData.getSqlWithLimit1(metaColumns, table);
            List<Pair<byte[], byte[]>> rangeList;
            try {
                dbConn = getConnection();
                ps = dbConn.prepareStatement(sql);
                resultSet = ps.executeQuery();
                rangeList = helper.getRangeList(ps);
                LOG.info("region's count = {}", rangeList.size());
            } catch (Exception e) {
                String message = String.format("failed to query rangeList, sql = %s, dbUrl = %s, properties = %s, e = %s", sql, dbUrl, GsonUtil.GSON.toJson(properties), ExceptionUtil.getErrorMessage(e));
                throw new RuntimeException(message, e);
            }finally {
                DbUtil.closeDbResources(resultSet, ps, dbConn, false);
            }
            if(rangeList.size() < minNumSplits){
                String message = String.format("region's count [%s] must be less than or equal to channel number [%s], please reduce [channel] in FlinkX config!", rangeList.size(), minNumSplits);
                throw new IllegalArgumentException(message);
            }
            List<List<Pair<byte[], byte[]>>> list = RangeSplitUtil.subListBySegment(rangeList, minNumSplits);
            InputSplit[] splits = new InputSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                splits[i] = new Phoenix5InputSplit(i, minNumSplits, new Vector<>(list.get(i)));
            }
            return splits;
        }else{
            return super.createInputSplitsInternal(minNumSplits);
        }
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        if(readFromHbase){
            try {
                Phoenix5DatabaseMeta metaData = (Phoenix5DatabaseMeta) this.databaseInterface;
                sql = metaData.getSqlWithLimit1(metaColumns, table);
                dbConn = getConnection();
                ps = dbConn.prepareStatement(sql);
                resultSet = ps.executeQuery();
                ResultSetMetaData meta = ps.getMetaData();
                columnCount = meta.getColumnCount();
                List<String> typeList = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    String name = meta.getColumnName(i);
                    String type = meta.getColumnTypeName(i);
                    typeList.add(type);
                    LOG.info("field count = {}, name = {}, type = {}", i, name, type);
                }
                helper.initInstanceList(typeList);
                Vector<Pair<byte[], byte[]>> keyRangeList = ((Phoenix5InputSplit) inputSplit).getSplits();

                scan = new Scan();
                keyRangeIterator = keyRangeList.iterator();
                Pair<byte[], byte[]> pair = keyRangeIterator.next();
                //for yarn session mode
                scan.setStartRow(pair.getLeft());
                scan.setStopRow(pair.getRight());
                scan.setFamilyMap(helper.getFamilyMap(resultSet));
                scan.setLoadColumnFamiliesOnDemand(true);
                scan.setAttribute("scanProjector", helper.getScanProjector(resultSet));
                scan.setAttribute("_NonAggregateQuery", QueryConstants.TRUE);
                scan.setCaching(scanCacheSize);
                scan.setBatch(scanBatchSize);

                Map<String, Object> map = helper.analyzePhoenixUrl(dbUrl);
                Configuration hConfiguration = HBaseConfiguration.create();
                hConfiguration.set(HConstants.ZOOKEEPER_QUORUM, (String) map.get("quorum"));
                Object port = map.get("port");
                if(port == null){
                    hConfiguration.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT);
                }else{
                    hConfiguration.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, (int)port);
                }
                Object rootNode = map.get("rootNode");
                if(rootNode == null){
                    hConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
                }else{
                    hConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, (String) rootNode);
                }
                hConfiguration.setBoolean(HConstants.CLUSTER_DISTRIBUTED, true);
                org.apache.hadoop.hbase.client.Connection hConn = ConnectionFactory.createConnection(hConfiguration);
                hTable = hConn.getTable(TableName.valueOf(table));
                resultIterator = hTable.getScanner(scan).iterator();
            } catch (Exception e) {
                String message = String.format("openInputFormat() failed, dbUrl = %s, properties = %s, e = %s", dbUrl, GsonUtil.GSON.toJson(properties), ExceptionUtil.getErrorMessage(e));
                throw new RuntimeException(message, e);
            }finally {
                DbUtil.closeDbResources(resultSet, ps, dbConn, false);
            }
        }else{
            super.openInternal(inputSplit);
        }
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        try {
            row = new Row(columnCount);
            if(readFromHbase){
                NoTagsKeyValue cell = (NoTagsKeyValue)resultIterator.next().listCells().get(0);
                return helper.getRow(cell.getBuffer(), cell.getOffset(), cell.getLength());
            }else{
                if (!hasNext) {
                    return null;
                }
                for (int pos = 0; pos < row.getArity(); pos++) {
                    row.setField(pos, clobToString(resultSet.getObject(pos + 1)));
                }
                return super.nextRecordInternal(row);
            }
        } catch (Exception e) {
            throw new IOException(String.format("Couldn't read data, e = %s", ExceptionUtil.getErrorMessage(e)), e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException{
        if(readFromHbase){
            if(resultIterator.hasNext()){
                return false;
            }else{
                if(keyRangeIterator.hasNext()){
                    Pair<byte[], byte[]> pair = keyRangeIterator.next();
                    LOG.info("switch regions, current region is [{}}] to [{}]", GsonUtil.GSON.toJson(pair.getLeft()), GsonUtil.GSON.toJson(pair.getRight()));
                    //for yarn session mode
                    scan.setStartRow(pair.getLeft());
                    scan.setStopRow(pair.getRight());
                    resultIterator = hTable.getScanner(scan).iterator();
                    return reachedEnd();
                }else{
                    return true;
                }
            }
        }else{
            return super.reachedEnd();
        }
    }

    /**
     * 获取数据库连接，用于子类覆盖
     * @return connection
     */
    @Override
    protected Connection getConnection() throws SQLException {
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
            if (urlFileName.startsWith("flinkx-phoenix5-reader")) {
                needJar.add(url);
                break;
            }
        }

        ClassLoader parentClassLoader = getClass().getClassLoader();
        List<String> list = new LinkedList<>();
        list.add("org.apache.flink");
        list.add("com.dtstack.flinkx");

        childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, list.toArray(new String[0]), FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER);

        ClassUtil.forName(driverName, childFirstClassLoader);
        if(StringUtils.isNotEmpty(username)){
            properties.setProperty("user", username);
        }
        if(StringUtils.isNotEmpty(password)){
            properties.setProperty("password", password);
        }

        try {
            helper = PhoenixUtil.getHelper(childFirstClassLoader);
            return helper.getConn(dbUrl, properties);
        } catch (IOException | CompileException e) {
            String message = String.format("cannot get phoenix connection, dbUrl = %s, properties = %s, e = %s", dbUrl, GsonUtil.GSON.toJson(properties), ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }
}