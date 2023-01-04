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

package com.dtstack.chunjun.connector.hive3.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hive3.inputSplit.HdfsTextInputSplit;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.List;

public class HdfsTextInputFormat extends BaseHdfsInputFormat {

    private static final long serialVersionUID = 2393219757331128551L;

    @Override
    protected InputSplit[] createHdfsSplit(int minNumSplits) throws IOException {
        initHadoopJobConf();
        // 是否在MapReduce中递归遍历Input目录
        hadoopJobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        FileInputFormat.setInputPathFilter(hadoopJobConf, HdfsPathFilter.class);
        FileInputFormat.setInputPaths(hadoopJobConf, hdfsConfig.getPath());
        TextInputFormat inputFormat = new TextInputFormat();
        inputFormat.configure(hadoopJobConf);
        org.apache.hadoop.mapred.InputSplit[] splits;
        try {
            splits = inputFormat.getSplits(hadoopJobConf, minNumSplits);
        } catch (IOException e) {
            throw new ChunJunRuntimeException("failed to get hdfs text splits", e);
        }
        if (splits != null) {
            HdfsTextInputSplit[] hdfsTextInputSplits = new HdfsTextInputSplit[splits.length];
            for (int i = 0; i < splits.length; ++i) {
                hdfsTextInputSplits[i] = new HdfsTextInputSplit(splits[i], i);
            }
            return hdfsTextInputSplits;
        }
        return null;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        if (super.openKerberos) {
            ugi.doAs(
                    (PrivilegedAction<Object>)
                            () -> {
                                try {
                                    initHdfsTextReader(inputSplit);
                                } catch (Exception e) {
                                    throw new ChunJunRuntimeException(
                                            "error to open Internal, split = " + inputSplit, e);
                                }
                                return null;
                            });
        } else {
            initHdfsTextReader(inputSplit);
        }
    }

    private void initHdfsTextReader(InputSplit inputSplit) {
        try {
            HdfsTextInputSplit hdfsTextInputSplit = (HdfsTextInputSplit) inputSplit;
            org.apache.hadoop.mapred.InputSplit fileSplit = hdfsTextInputSplit.getTextSplit();
            findCurrentPartition(((FileSplit) fileSplit).getPath());
            recordReader = inputFormat.getRecordReader(fileSplit, hadoopJobConf, Reporter.NULL);
            key = new LongWritable();
            value = new Text();
        } catch (Exception e) {
            throw new ChunJunRuntimeException("error to open Internal, split = " + inputSplit, e);
        }
    }

    @Override
    public InputFormat<LongWritable, Text> createMapredInputFormat() {
        return new TextInputFormat();
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
            String line =
                    new String(
                            ((Text) value).getBytes(),
                            0,
                            ((Text) value).getLength(),
                            StandardCharsets.UTF_8);
            String[] fields =
                    StringUtils.splitPreserveAllTokens(line, hdfsConfig.getFieldDelimiter());
            GenericRowData genericRowData =
                    new GenericRowData(Math.max(fields.length, fieldConfList.size()));
            if (fieldConfList.size() == 1
                    && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {
                for (int i = 0; i < fields.length; i++) {
                    genericRowData.setField(i, fields[i]);
                }
            } else {
                for (int i = 0; i < fieldConfList.size(); i++) {
                    FieldConfig fieldConfig = fieldConfList.get(i);

                    Object value = null;
                    if (fieldConfig.getValue() != null) {
                        value = fieldConfig.getValue();
                    } else if (fieldConfig.getIndex() != null
                            && fieldConfig.getIndex() < fields.length) {
                        String strVal = fields[fieldConfig.getIndex()];
                        if (!Hive3Util.NULL_VALUE.equals(strVal)) {
                            value = strVal;
                        }
                    }
                    genericRowData.setField(i, value);
                }
            }
            rowData = rowConverter.toInternal(genericRowData);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
        return rowData;
    }
}
