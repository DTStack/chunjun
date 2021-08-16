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
package com.dtstack.flinkx.connector.phoenix5.util;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.phoenix5.conf.Phoenix5Conf;
import com.dtstack.flinkx.connector.phoenix5.converter.HBaseColumnConverter;
import com.dtstack.flinkx.connector.phoenix5.converter.HBaseRawTypeConverter;
import com.dtstack.flinkx.connector.phoenix5.converter.HBaseRowConverter;
import com.dtstack.flinkx.connector.phoenix5.source.Phoenix5InputSplit;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Vector;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5Helper implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Phoenix5Helper.class);
    public transient RowProjector rowProjector;
    public List<PDataType> phoenixDataTypeList;
    public AbstractRowConverter rowConverter;
    private StatementContext context;
    private transient Table hTable;
    private transient Scan scan = null;
    private transient Iterator<Pair<byte[], byte[]>> keyRangeIterator;

    public List<Pair<byte[], byte[]>> getRangeList(Phoenix5Conf conf, Connection conn) {

        final List<FieldConf> metaColumns = conf.getColumn();
        List<String> columnNameList = new ArrayList<>(metaColumns.size());
        metaColumns.forEach(columnName -> columnNameList.add(columnName.getName()));
        String sql = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            // get a query statement with limit 0.
            sql = Phoenix5Util.getSqlWithLimit0(columnNameList, conf.getTable());
            ps = conn.prepareStatement(sql);
            resultSet = ps.executeQuery();
            // handle splits.
            List<KeyRange> rangeList = ((PhoenixPreparedStatement) ps).getQueryPlan().getSplits();
            List<Pair<byte[], byte[]>> list = new ArrayList(rangeList.size());
            for (KeyRange keyRange : rangeList) {
                list.add(Pair.of(keyRange.getLowerRange(), keyRange.getUpperRange()));
            }
        } catch (SQLException e) {
            String message =
                    String.format(
                            "failed to query rangeList, sql = %s, dbUrl = %s, properties = %s, e = %s",
                            sql,
                            conf.getJdbcUrl(),
                            GsonUtil.GSON.toJson(conf),
                            ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        } finally {
            Phoenix5Util.closeDbResources(resultSet, ps, conn);
        }
        return getRangeList(ps);
    }

    public List<Pair<byte[], byte[]>> getRangeList(PreparedStatement ps) {
        List<KeyRange> rangeList = ((PhoenixPreparedStatement) ps).getQueryPlan().getSplits();
        List<Pair<byte[], byte[]>> list = new ArrayList(rangeList.size());
        for (KeyRange keyRange : rangeList) {
            list.add(Pair.of(keyRange.getLowerRange(), keyRange.getUpperRange()));
        }
        return list;
    }

    public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
        return context.getScan().getFamilyMap();
    }

    public void initInstanceList(List<String> typeList) {
        phoenixDataTypeList = new ArrayList(typeList.size());
        for (String type : typeList) {
            phoenixDataTypeList.add(getPDataType(type));
        }
    }

    public void initMetaData(Phoenix5Conf conf, Connection dbConn) {

        List<FieldConf> metaColumns = conf.getColumn();
        String tableName = conf.getTable();

        Preconditions.checkNotNull(metaColumns, "meta Columns must not be null.");
        Preconditions.checkNotNull(tableName, "tableName must not be null.");
        Preconditions.checkNotNull(dbConn, "phoenix connection must not be null.");

        List<String> columnNameList = new ArrayList<>(metaColumns.size());
        metaColumns.forEach(columnName -> columnNameList.add(columnName.getName()));
        String sql;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        ResultSetMetaData meta = null;
        int columnCount;
        try {
            // retrieve once table for obtain metadata(column name and type).
            sql = Phoenix5Util.getSqlWithLimit0(columnNameList, tableName);
            ps = dbConn.prepareStatement(sql);
            resultSet = ps.executeQuery();
            meta = ps.getMetaData();
            columnCount = meta.getColumnCount();
            List<String> fullColumnNameList = new ArrayList<>(columnCount);
            List<String> fullColumnTypeList = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                String name = meta.getColumnName(i);
                String type = meta.getColumnTypeName(i);
                fullColumnNameList.add(name);
                fullColumnTypeList.add(type);
                LOG.info("field count, name = {}, type = {}", i + "," + name, type);
            }

            initInstanceList(fullColumnTypeList);
            initStatementContext(resultSet);
            initRowProjector(resultSet);
            initRowConvert(conf, fullColumnNameList, fullColumnTypeList);
        } catch (SQLException throwables) {
            throw new FlinkxRuntimeException(
                    String.format(
                            "error to get meta from [%s.%s]",
                            meta != null ? meta.toString() : "meta is null", tableName));
        } finally {
            Phoenix5Util.closeDbResources(resultSet, ps, dbConn);
        }
    }

    private void initRowConvert(
            Phoenix5Conf conf, List<String> fullColumnNameList, List<String> fullColumnTypeList) {
        final Boolean syncTaskType = conf.getSyncTaskType();
        RowType rowType =
                TableUtil.createRowType(
                        fullColumnNameList, fullColumnTypeList, HBaseRawTypeConverter::apply);
        // sync task
        if (syncTaskType) {
            rowConverter = new HBaseColumnConverter(rowType, rowProjector);
        } else {
            // sql task
            rowConverter = new HBaseRowConverter(rowType, rowProjector);
        }
    }

    private void initStatementContext(ResultSet resultSet) {
        context = ((PhoenixResultSet) resultSet).getContext();
    }

    private void initRowProjector(ResultSet resultSet) {
        Field field = null;
        try {
            field = PhoenixResultSet.class.getDeclaredField("rowProjector");
            field.setAccessible(true);
            rowProjector = (RowProjector) field.get(resultSet);
            field.setAccessible(false);
        } catch (NoSuchFieldException e) {
            throw new FlinkxRuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new FlinkxRuntimeException(e);
        }
    }

    public byte[] getScanProjector() {
        return context.getScan().getAttribute("scanProjector");
    }

    public Iterator<Result> getHbaseIterator(InputSplit inputSplit, Phoenix5Conf conf)
            throws IOException {
        return getResultScanner(inputSplit, conf).iterator();
    }

    public ResultScanner getResultScanner(InputSplit inputSplit, Phoenix5Conf conf)
            throws IOException {
        Vector<Pair<byte[], byte[]>> keyRangeList = ((Phoenix5InputSplit) inputSplit).getSplits();
        Map<String, Object> map = null;
        try {
            map = analyzePhoenixUrl(conf.getJdbcUrl());
        } catch (SQLException e) {
            throw new FlinkxRuntimeException(e);
        }
        Configuration hConfiguration = HBaseConfiguration.create();
        hConfiguration.set(HConstants.ZOOKEEPER_QUORUM, (String) map.get("quorum"));
        Object port = map.get("port");
        if (port == null) {
            hConfiguration.setInt(
                    HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT,
                    HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT);
        } else {
            hConfiguration.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, (int) port);
        }
        Object rootNode = map.get("rootNode");
        if (rootNode == null) {
            hConfiguration.set(
                    HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        } else {
            hConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, (String) rootNode);
        }
        hConfiguration.setBoolean(HConstants.CLUSTER_DISTRIBUTED, true);
        try {
            org.apache.hadoop.hbase.client.Connection hConn =
                    ConnectionFactory.createConnection(hConfiguration);
            hTable = hConn.getTable(TableName.valueOf(conf.getTable()));
        } catch (IOException e) {
            throw new FlinkxRuntimeException(
                    "Obtain org.apache.hadoop.hbase.client.Connection fail.", e);
        }
        return hTable.getScanner(getScan(keyRangeList, conf));
    }

    public Scan getScan(Vector<Pair<byte[], byte[]>> keyRangeList, Phoenix5Conf conf) {
        scan = new Scan();
        keyRangeIterator = keyRangeList.iterator();
        Pair<byte[], byte[]> pair = keyRangeIterator.next();
        // for yarn session mode
        scan.setStartRow(pair.getLeft());
        scan.setStopRow(pair.getRight());
        scan.setFamilyMap(getFamilyMap());
        scan.setLoadColumnFamiliesOnDemand(true);
        scan.setAttribute("scanProjector", getScanProjector());
        scan.setAttribute("_NonAggregateQuery", QueryConstants.TRUE);

        Integer scanCacheSize =
                conf.getScanCacheSize() != null
                        ? conf.getScanCacheSize()
                        : HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;

        Integer scanBatchSize = conf.getScanBatchSize() != null ? conf.getScanBatchSize() : -1;

        scan.setCaching(scanCacheSize);
        scan.setBatch(scanBatchSize);

        return scan;
    }

    public void setStartStop(Pair<byte[], byte[]> pair) {
        if (null == scan) {
            throw new NullPointerException(String.format("error scan start or stop "));
        }
        byte[] start = pair.getLeft();
        byte[] stop = pair.getRight();
        // for yarn session mode
        scan.setStartRow(start);
        scan.setStopRow(stop);
        LOG.info(
                "switch regions, current region is [{}}] to [{}]",
                GsonUtil.GSON.toJson(pair.getLeft()),
                GsonUtil.GSON.toJson(pair.getRight()));
    }

    public Boolean hasNext() {
        if (null == keyRangeIterator) {
            return false;
        }
        return keyRangeIterator.hasNext();
    }

    public Iterator<Result> next() throws IOException {
        final Pair<byte[], byte[]> startStop = keyRangeIterator.next();
        setStartStop(startStop);
        return hTable.getScanner(scan).iterator();
    }

    public Map<String, Object> analyzePhoenixUrl(String url) throws SQLException {
        PhoenixEmbeddedDriver.ConnectionInfo info =
                PhoenixEmbeddedDriver.ConnectionInfo.create(url);
        Map<String, Object> map = new HashMap(8);
        // zk地址
        map.put("quorum", info.getZookeeperQuorum());
        // zk端口
        map.put("port", info.getPort());
        // hbase zk节点名称
        map.put("rootNode", info.getRootNode());
        map.put("principal", info.getPrincipal());
        map.put("keytabFile", info.getKeytab());
        return map;
    }

    /**
     * 根据字段类型获取Phoenix转换实例 phoenix支持以下数据类型
     *
     * @param type
     * @return
     */
    public PDataType getPDataType(String type) {
        if (StringUtils.isBlank(type)) {
            throw new RuntimeException("type[" + type + "] cannot be blank!");
        }
        switch (type.toUpperCase()) {
            case "INTEGER":
                return PInteger.INSTANCE;
            case "UNSIGNED_INT":
                return PUnsignedInt.INSTANCE;
            case "BIGINT":
                return PLong.INSTANCE;
            case "UNSIGNED_LONG":
                return PUnsignedLong.INSTANCE;
            case "TINYINT":
                return PTinyint.INSTANCE;
            case "UNSIGNED_TINYINT":
                return PUnsignedTinyint.INSTANCE;
            case "SMALLINT":
                return PSmallint.INSTANCE;
            case "UNSIGNED_SMALLINT":
                return PUnsignedSmallint.INSTANCE;
            case "FLOAT":
                return PFloat.INSTANCE;
            case "UNSIGNED_FLOAT":
                return PUnsignedFloat.INSTANCE;
            case "DOUBLE":
                return PDouble.INSTANCE;
            case "UNSIGNED_DOUBLE":
                return PUnsignedDouble.INSTANCE;
            case "DECIMAL":
                return PDecimal.INSTANCE;
            case "BOOLEAN":
                return PBoolean.INSTANCE;
            case "TIME":
                return PTime.INSTANCE;
            case "DATE":
                return PDate.INSTANCE;
            case "TIMESTAMP":
                return PTimestamp.INSTANCE;
            case "UNSIGNED_TIME":
                return PUnsignedTime.INSTANCE;
            case "UNSIGNED_DATE":
                return PUnsignedDate.INSTANCE;
            case "UNSIGNED_TIMESTAMP":
                return PUnsignedTimestamp.INSTANCE;
            case "VARCHAR":
                return PVarchar.INSTANCE;
            case "CHAR":
                return PChar.INSTANCE;
                // not support BINARY VARBINARY.
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    public RowData toInternal(NoTagsKeyValue cell) throws Exception {
        return rowConverter.toInternal(cell);
    }
}
