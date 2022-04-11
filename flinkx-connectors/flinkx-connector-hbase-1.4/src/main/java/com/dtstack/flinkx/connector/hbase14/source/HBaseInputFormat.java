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

package com.dtstack.flinkx.connector.hbase14.source;

import com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils;
import com.dtstack.flinkx.connector.hbase14.util.HBaseHelper;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Maps;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The InputFormat Implementation used for HbaseReader
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseInputFormat extends BaseRichInputFormat {

    public static final String KEY_ROW_KEY = "rowkey";

    protected Map<String, Object> hbaseConfig;
    protected String tableName;
    protected String startRowkey;
    protected String endRowkey;
    protected List<String> columnNames;
    protected List<String> columnValues;
    protected List<String> columnFormats;
    protected List<String> columnTypes;
    protected boolean isBinaryRowkey;
    protected String encoding;
    /** 客户端每次 rpc fetch 的行数 */
    protected int scanCacheSize = 1000;

    private transient Connection connection;
    private transient Scan scan;
    private transient Table table;
    private transient ResultScanner resultScanner;
    private transient Result next;
    private transient Map<String, byte[][]> nameMaps;

    private boolean openKerberos = false;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        LOG.info("HbaseOutputFormat openInputFormat start");
        nameMaps = Maps.newConcurrentMap();

        connection = HBaseHelper.getHbaseConnection(hbaseConfig);

        LOG.info("HbaseOutputFormat openInputFormat end");
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        try (Connection connection = HBaseHelper.getHbaseConnection(hbaseConfig)) {
            if (HBaseConfigUtils.isEnableKerberos(hbaseConfig)) {
                UserGroupInformation ugi = HBaseHelper.getUgi(hbaseConfig);
                return ugi.doAs(
                        (PrivilegedAction<HBaseInputSplit[]>)
                                () ->
                                        split(
                                                connection,
                                                tableName,
                                                startRowkey,
                                                endRowkey,
                                                isBinaryRowkey));
            } else {
                return split(connection, tableName, startRowkey, endRowkey, isBinaryRowkey);
            }
        }
    }

    public HBaseInputSplit[] split(
            Connection hConn,
            String tableName,
            String startKey,
            String endKey,
            boolean isBinaryRowkey) {
        byte[] startRowkeyByte = HBaseHelper.convertRowKey(startKey, isBinaryRowkey);
        byte[] endRowkeyByte = HBaseHelper.convertRowKey(endKey, isBinaryRowkey);

        /* 如果用户配置了 startRowkey 和 endRowkey，需要确保：startRowkey <= endRowkey */
        if (startRowkeyByte.length != 0
                && endRowkeyByte.length != 0
                && Bytes.compareTo(startRowkeyByte, endRowkeyByte) > 0) {
            throw new IllegalArgumentException("startRowKey can't be bigger than endRowkey");
        }

        RegionLocator regionLocator = HBaseHelper.getRegionLocator(hConn, tableName);
        List<HBaseInputSplit> resultSplits;
        try {
            Pair<byte[][], byte[][]> regionRanges = regionLocator.getStartEndKeys();
            if (null == regionRanges) {
                throw new RuntimeException("Failed to retrieve rowkey ragne");
            }
            resultSplits = doSplit(startRowkeyByte, endRowkeyByte, regionRanges);

            LOG.info("HBaseReader split job into {} tasks.", resultSplits.size());
            return resultSplits.toArray(new HBaseInputSplit[resultSplits.size()]);
        } catch (Exception e) {
            throw new RuntimeException("Failed to split hbase table");
        } finally {
            HBaseHelper.closeRegionLocator(regionLocator);
        }
    }

    private List<HBaseInputSplit> doSplit(
            byte[] startRowkeyByte, byte[] endRowkeyByte, Pair<byte[][], byte[][]> regionRanges) {

        List<HBaseInputSplit> configurations = new ArrayList<>();

        for (int i = 0; i < regionRanges.getFirst().length; i++) {

            byte[] regionStartKey = regionRanges.getFirst()[i];
            byte[] regionEndKey = regionRanges.getSecond()[i];

            // 当前的region为最后一个region
            // 如果最后一个region的start Key大于用户指定的userEndKey,则最后一个region，应该不包含在内
            // 注意如果用户指定userEndKey为"",则此判断应该不成立。userEndKey为""表示取得最大的region
            boolean isSkip =
                    Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0
                            && (endRowkeyByte.length != 0
                                    && (Bytes.compareTo(regionStartKey, endRowkeyByte) > 0));
            if (isSkip) {
                continue;
            }

            // 如果当前的region不是最后一个region，
            // 用户配置的userStartKey大于等于region的endkey,则这个region不应该含在内
            if ((Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) != 0)
                    && (Bytes.compareTo(startRowkeyByte, regionEndKey) >= 0)) {
                continue;
            }

            // 如果用户配置的userEndKey小于等于 region的startkey,则这个region不应该含在内
            // 注意如果用户指定的userEndKey为"",则次判断应该不成立。userEndKey为""表示取得最大的region
            if (endRowkeyByte.length != 0
                    && (Bytes.compareTo(endRowkeyByte, regionStartKey) <= 0)) {
                continue;
            }

            String thisStartKey = getStartKey(startRowkeyByte, regionStartKey);
            String thisEndKey = getEndKey(endRowkeyByte, regionEndKey);
            HBaseInputSplit hbaseInputSplit = new HBaseInputSplit(thisStartKey, thisEndKey);
            configurations.add(hbaseInputSplit);
        }

        return configurations;
    }

    private String getEndKey(byte[] endRowkeyByte, byte[] regionEndKey) {
        // 由于之前处理过，所以传入的userStartKey不可能为null
        if (endRowkeyByte == null) {
            throw new IllegalArgumentException("userEndKey should not be null!");
        }

        byte[] tempEndRowkeyByte;

        if (endRowkeyByte.length == 0) {
            tempEndRowkeyByte = regionEndKey;
        } else if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
            // 为最后一个region
            tempEndRowkeyByte = endRowkeyByte;
        } else {
            if (Bytes.compareTo(endRowkeyByte, regionEndKey) > 0) {
                tempEndRowkeyByte = regionEndKey;
            } else {
                tempEndRowkeyByte = endRowkeyByte;
            }
        }

        return Bytes.toStringBinary(tempEndRowkeyByte);
    }

    private String getStartKey(byte[] startRowkeyByte, byte[] regionStarKey) {
        // 由于之前处理过，所以传入的userStartKey不可能为null
        if (startRowkeyByte == null) {
            throw new IllegalArgumentException("userStartKey should not be null!");
        }

        byte[] tempStartRowkeyByte;

        if (Bytes.compareTo(startRowkeyByte, regionStarKey) < 0) {
            tempStartRowkeyByte = regionStarKey;
        } else {
            tempStartRowkeyByte = startRowkeyByte;
        }
        return Bytes.toStringBinary(tempStartRowkeyByte);
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        HBaseInputSplit hbaseInputSplit = (HBaseInputSplit) inputSplit;
        byte[] startRow = Bytes.toBytesBinary(hbaseInputSplit.getStartkey());
        byte[] stopRow = Bytes.toBytesBinary(hbaseInputSplit.getEndKey());

        if (null == connection || connection.isClosed()) {
            connection = HBaseHelper.getHbaseConnection(hbaseConfig);
        }

        openKerberos = HBaseConfigUtils.isEnableKerberos(hbaseConfig);

        table = connection.getTable(TableName.valueOf(tableName));
        scan = new Scan();
        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
        scan.setCaching(scanCacheSize);
        resultScanner = table.getScanner(scan);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        next = resultScanner.next();
        return next == null;
    }

    @Override
    public RowData nextRecordInternal(RowData rawRow) {
        GenericRowData row = new GenericRowData(columnTypes.size());

        for (int i = 0; i < columnTypes.size(); ++i) {
            String columnType = columnTypes.get(i);
            String columnName = columnNames.get(i);
            String columnFormat = columnFormats.get(i);
            String columnValue = columnValues.get(i);
            Object col = null;
            byte[] bytes;

            try {
                if (StringUtils.isNotEmpty(columnValue)) {
                    // 常量
                    col = convertValueToAssignType(columnType, columnValue, columnFormat);
                } else {
                    if (KEY_ROW_KEY.equals(columnName)) {
                        bytes = next.getRow();
                    } else {
                        byte[][] arr = nameMaps.get(columnName);
                        if (arr == null) {
                            arr = new byte[2][];
                            String[] arr1 = columnName.split(":");
                            arr[0] = arr1[0].trim().getBytes(StandardCharsets.UTF_8);
                            arr[1] = arr1[1].trim().getBytes(StandardCharsets.UTF_8);
                            nameMaps.put(columnName, arr);
                        }
                        bytes = next.getValue(arr[0], arr[1]);
                    }
                    col = convertBytesToAssignType(columnType, bytes, columnFormat);
                }
                row.setField(i, col);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return row;
    }

    @Override
    public void closeInternal() throws IOException {
        HBaseHelper.closeConnection(connection);
    }

    public Object convertValueToAssignType(
            String columnType, String constantValue, String dateformat) throws Exception {
        Object column = null;
        if (org.apache.commons.lang3.StringUtils.isEmpty(constantValue)) {
            return column;
        }

        switch (columnType.toUpperCase()) {
            case "BOOLEAN":
                column = Boolean.valueOf(constantValue);
                break;
            case "SHORT":
            case "INT":
            case "LONG":
                column = NumberUtils.createBigDecimal(constantValue).toBigInteger();
                break;
            case "FLOAT":
            case "DOUBLE":
                column = new BigDecimal(constantValue);
                break;
            case "STRING":
                column = constantValue;
                break;
            case "DATE":
                column = DateUtils.parseDate(constantValue, new String[] {dateformat});
                break;
            default:
                throw new IllegalArgumentException("Unsupported columnType: " + columnType);
        }

        return column;
    }

    public Object convertBytesToAssignType(String columnType, byte[] byteArray, String dateformat)
            throws Exception {
        Object column = null;
        if (ArrayUtils.isEmpty(byteArray)) {
            return null;
        }
        String bytesToString = new String(byteArray, encoding);
        switch (columnType.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                column = Boolean.valueOf(bytesToString);
                break;
            case "SHORT":
                column = Short.valueOf(bytesToString);
                break;
            case "INT":
                column = Integer.valueOf(bytesToString);
                break;
            case "LONG":
                column = Long.valueOf(bytesToString);
                break;
            case "FLOAT":
                column = Float.valueOf(bytesToString);
                break;
            case "DOUBLE":
                column = Double.valueOf(bytesToString);
                break;
            case "STRING":
                column = bytesToString;
                break;
            case "BINARY_STRING":
                column = Bytes.toStringBinary(byteArray);
                break;
            case "DATE":
                String dateValue = Bytes.toStringBinary(byteArray);
                column = DateUtils.parseDate(dateValue, new String[] {dateformat});
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }
        return column;
    }
}
