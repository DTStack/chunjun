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

package com.dtstack.chunjun.connector.hbase14.source;

import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.util.HBaseConfigUtils;
import com.dtstack.chunjun.connector.hbase14.util.HBaseHelper;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

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
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The InputFormat Implementation used for HbaseReader
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseInputFormat extends BaseRichInputFormat {

    protected Map<String, Object> hbaseConfig;
    protected HBaseConf hBaseConf;

    private transient Connection connection;
    private transient Scan scan;
    private transient Table table;
    private transient ResultScanner resultScanner;
    private transient Result next;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        LOG.info("HbaseOutputFormat openInputFormat start");

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
                                                hBaseConf.getTable(),
                                                hBaseConf.getStartRowkey(),
                                                hBaseConf.getEndRowkey(),
                                                hBaseConf.isBinaryRowkey()));
            } else {
                return split(
                        connection,
                        hBaseConf.getTable(),
                        hBaseConf.getStartRowkey(),
                        hBaseConf.getEndRowkey(),
                        hBaseConf.isBinaryRowkey());
            }
        }
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        HBaseInputSplit hbaseInputSplit = (HBaseInputSplit) inputSplit;
        byte[] startRow = Bytes.toBytesBinary(hbaseInputSplit.getStartkey());
        byte[] stopRow = Bytes.toBytesBinary(hbaseInputSplit.getEndKey());

        if (null == connection || connection.isClosed()) {
            connection = HBaseHelper.getHbaseConnection(hbaseConfig);
        }

        table = connection.getTable(TableName.valueOf(hBaseConf.getTable()));
        scan = new Scan();
        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
        scan.setCaching(hBaseConf.getScanCacheSize());
        resultScanner = table.getScanner(scan);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        next = resultScanner.next();
        return next == null;
    }

    @Override
    public RowData nextRecordInternal(RowData rawRow) throws ReadRecordException {
        try {
            rawRow = rowConverter.toInternal(next);
            return rawRow;
        } catch (Exception se) {
            throw new ReadRecordException("", se, 0, rawRow);
        }
    }

    @Override
    public void closeInternal() throws IOException {
        HBaseHelper.closeConnection(connection);
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
}
