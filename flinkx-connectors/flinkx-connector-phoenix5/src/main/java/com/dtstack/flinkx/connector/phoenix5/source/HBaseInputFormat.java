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

package com.dtstack.flinkx.connector.phoenix5.source;

import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.phoenix5.conf.Phoenix5Conf;
import com.dtstack.flinkx.connector.phoenix5.util.Phoenix5Helper;
import com.dtstack.flinkx.connector.phoenix5.util.Phoenix5Util;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.ReadRecordException;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.RangeSplitUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class HBaseInputFormat extends JdbcInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseInputFormat.class);

    private transient Iterator<Result> resultIterator;
    private Phoenix5Conf phoenix5Conf;
    private transient Phoenix5Helper phoenix5Helper;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        Connection conn = getConnection();
        phoenix5Helper = new Phoenix5Helper();
        LOG.info(
                "phoenix5reader config [readFromHbase] is true, FlinkX will read data from HBase directly!");
        List<Pair<byte[], byte[]>> rangeList = phoenix5Helper.getRangeList(phoenix5Conf, conn);
        LOG.info("region's count = {}", rangeList.size());
        InputSplit[] splits = getInputSplits(minNumSplits, rangeList);
        return splits;
    }

    private InputSplit[] getInputSplits(int minNumSplits, List<Pair<byte[], byte[]>> rangeList) {
        if (rangeList.size() < minNumSplits) {
            String message =
                    String.format(
                            "region's count [%s] must be less than or equal to channel number [%s], please reduce [channel] in FlinkX config!",
                            rangeList.size(), minNumSplits);
            throw new IllegalArgumentException(message);
        }
        List<List<Pair<byte[], byte[]>>> list =
                RangeSplitUtil.subListBySegment(rangeList, minNumSplits);
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new Phoenix5InputSplit(i, minNumSplits, new Vector<>(list.get(i)));
        }
        return splits;
    }

    @Override
    public void openInternal(InputSplit inputSplit) {
        LOG.info("inputSplit = {}", inputSplit);
        Phoenix5Conf conf = phoenix5Conf;
        phoenix5Helper = new Phoenix5Helper();
        phoenix5Helper.initMetaData(conf, getConnection());
        try {
            resultIterator = phoenix5Helper.getHbaseIterator(inputSplit, conf);
        } catch (Exception e) {
            String message =
                    String.format(
                            "openInputFormat() failed, dbUrl = %s, properties = %s, e = %s",
                            conf.getJdbcUrl(),
                            GsonUtil.GSON.toJson(conf),
                            ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }

    // phoenix resolve table meta data by retrieving a row of data.
    @Override
    protected Pair<List<String>, List<String>> getTableMetaData() {
        return Phoenix5Util.getTableMetaData(
                phoenix5Conf.getColumn(), phoenix5Conf.getTable(), getConnection());
    }

    @Override
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        NoTagsKeyValue cell = (NoTagsKeyValue) resultIterator.next().listCells().get(0);
        try {
            rowData = phoenix5Helper.toInternal(cell);
            return rowData;
        } catch (Exception e) {
            throw new ReadRecordException(
                    String.format("Couldn't read data, e = %s", ExceptionUtil.getErrorMessage(e)),
                    e);
        }
    }

    @Override
    public boolean reachedEnd() {
        if (resultIterator.hasNext()) {
            return false;
        } else {
            if (phoenix5Helper.hasNext()) {
                try {
                    resultIterator = phoenix5Helper.next();
                } catch (IOException e) {
                    throw new FlinkxRuntimeException(e);
                }
                return reachedEnd();
            } else {
                return true;
            }
        }
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return connection
     */
    @SuppressWarnings("AlibabaRemoveCommentedCode")
    @Override
    protected Connection getConnection() {
        Connection conn =
                Phoenix5Util.getConnection("org.apache.phoenix.jdbc.PhoenixDriver", phoenix5Conf);
        return conn;
    }

    @Override
    public void closeInternal() {
        super.closeInternal();
    }

    public void setPhoenix5Conf(Phoenix5Conf phoenix5Conf) {
        this.phoenix5Conf = phoenix5Conf;
    }

    public Phoenix5Conf getPhoenix5Conf() {
        return phoenix5Conf;
    }
}
