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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PDataType;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
    private List<PDataType> instanceList;
    private transient RowProjector rowProjector;
    private transient Scan scan;
    private transient Table hTable;
    private transient Iterator<Pair<byte[], byte[]>> keyRangeIterator;
    private transient Iterator<Result> resultIterator;

    private transient URLClassLoader childFirstClassLoader;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }

    @Override
    @SuppressWarnings("unchecked")
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        if(readFromHbase){
            LOG.warn("phoenix5reader config [readFromHbase] is true, FlinkX will read data from HBase directly!");
            Phoenix5DatabaseMeta metaData = (Phoenix5DatabaseMeta) this.databaseInterface;
            sql = metaData.getSqlWithLimit1(metaColumns, table);
            List<KeyRange> rangeList;
            try {
                dbConn = getConnection();
                ps = dbConn.prepareStatement(sql);
                resultSet = ps.executeQuery();
                //类加载器不同，不能强转，不能反射调方法，只能反射拿私有字段
                Class<?> aClass = Class.forName("org.apache.phoenix.jdbc.PhoenixStatement", false, childFirstClassLoader);
                Field lastQueryPlan = aClass.getDeclaredField("lastQueryPlan");
                lastQueryPlan.setAccessible(true);

                Object queryPlan = lastQueryPlan.get(ps);
                lastQueryPlan.setAccessible(false);
                Field splits = ScanPlan.class.getDeclaredField("splits");
                splits.setAccessible(true);
                rangeList = (List<KeyRange>)splits.get(queryPlan);
                splits.setAccessible(false);
                LOG.info("region count = {}", rangeList.size());
            } catch (Exception e) {
                String message = String.format("failed to query rangeList, sl = %s dbUrl = %s, properties = %s, e = %s", sql, dbUrl, GsonUtil.GSON.toJson(properties), ExceptionUtil.getErrorMessage(e));
                throw new RuntimeException(message, e);
            }finally {
                DbUtil.closeDbResources(resultSet, ps, dbConn, false);
            }

            List<List<KeyRange>> list = RangeSplitUtil.subListBySegment(rangeList, minNumSplits);
            InputSplit[] splits = new InputSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                List<KeyRange> keyRangeList = list.get(i);
                Vector<Pair<byte[], byte[]>> vector = new Vector<>(keyRangeList.size());
                for (KeyRange keyRange : keyRangeList) {
                    vector.add(Pair.of(keyRange.getLowerRange(), keyRange.getUpperRange()));
                }
                splits[i] = new Phoenix5InputSplit(i, minNumSplits, vector);
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
                instanceList = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    String name = meta.getColumnName(i);
                    String type = meta.getColumnTypeName(i);
                    instanceList.add(PhoenixUtil.getPDataType(type));
                    LOG.info("field count = {}, name = {}, type = {}", i, name, type);
                }

                Field field = PhoenixResultSet.class.getDeclaredField("rowProjector");
                field.setAccessible(true);
                rowProjector = (RowProjector) field.get(resultSet);
                field.setAccessible(false);

                StatementContext context = ((PhoenixResultSet)resultSet).getContext();

                Vector<Pair<byte[], byte[]>> keyRangeList = ((Phoenix5InputSplit) inputSplit).getSplits();

                scan = new Scan();
                keyRangeIterator = keyRangeList.iterator();
                Pair<byte[], byte[]> pair = keyRangeIterator.next();
                scan.withStartRow(pair.getLeft());
                scan.withStopRow(pair.getRight());
                scan.setFamilyMap(context.getScan().getFamilyMap());
                scan.setLoadColumnFamiliesOnDemand(true);
                scan.setAttribute("scanProjector", context.getScan().getAttribute("scanProjector"));
                scan.setAttribute("_NonAggregateQuery", QueryConstants.TRUE);
                scan.setCaching(scanCacheSize);
                scan.setBatch(scanBatchSize);

                PhoenixEmbeddedDriver.ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(dbUrl);
                Configuration hConfiguration = HBaseConfiguration.create();
                hConfiguration.set(HConstants.ZOOKEEPER_QUORUM, connectionInfo.getZookeeperQuorum());
                hConfiguration.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, connectionInfo.getPort());
                if(StringUtils.isBlank(connectionInfo.getRootNode())){
                    hConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
                }else{
                    hConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, connectionInfo.getRootNode());
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
                List<Cell> cells = resultIterator.next().listCells();
                ResultTuple resultTuple = new ResultTuple(Result.create(Collections.singletonList(cells.get(0))));
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                for (int pos = 0; pos < columnCount; pos++) {
                    row.setField(pos, rowProjector.getColumnProjector(pos).getValue(resultTuple, instanceList.get(pos), ptr));
                }
                return row;
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
                    LOG.warn("switch regions...");
                    Pair<byte[], byte[]> pair = keyRangeIterator.next();
                    scan.withStartRow(pair.getLeft());
                    scan.withStopRow(pair.getRight());
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
        String[] alwaysParentFirstPatterns = new String[2];
        alwaysParentFirstPatterns[0] = "org.apache.flink";
        alwaysParentFirstPatterns[1] = "com.dtstack.flinkx";
        childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, alwaysParentFirstPatterns);

        ClassUtil.forName(driverName, childFirstClassLoader);
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        try {
            return PhoenixUtil.getConnectionInternal(dbUrl, properties, childFirstClassLoader);
        } catch (IOException | CompileException e) {
            String message = String.format("cannot get phoenix connection, dbUrl = %s, properties = %s, e = %s", dbUrl, GsonUtil.GSON.toJson(properties), ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }
}