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
package com.dtstack.chunjun.connector.hdfs.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hdfs.InputSplit.HdfsTextInputSplit;
import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HdfsTextInputFormat extends BaseHdfsInputFormat {

    private static final long serialVersionUID = -1740154546581800492L;

    @Override
    public InputSplit[] createHdfsSplit(int minNumSplits) throws IOException {
        super.initHadoopJobConf();
        org.apache.hadoop.mapred.FileInputFormat.setInputPathFilter(
                hadoopJobConf, HdfsPathFilter.class);

        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(hadoopJobConf, hdfsConfig.getPath());
        TextInputFormat inputFormat = new TextInputFormat();

        // 是否在MapReduce中递归遍历Input目录
        hadoopJobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        inputFormat.configure(hadoopJobConf);
        org.apache.hadoop.mapred.InputSplit[] splits =
                inputFormat.getSplits(hadoopJobConf, minNumSplits);

        if (splits != null) {
            List<HdfsTextInputSplit> splitList = new ArrayList<>();
            for (int i = 0; i < splits.length; ++i) {
                HdfsTextInputSplit split = new HdfsTextInputSplit(splits[i], i);
                if (split.getTextSplit().getLength() == 0) {
                    continue;
                }
                splitList.add(split);
            }
            return splitList.toArray(new HdfsTextInputSplit[splitList.size()]);
        }
        return null;
    }

    @Override
    public InputFormat createInputFormat() {
        return new TextInputFormat();
    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
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

    private void initHdfsTextReader(InputSplit inputSplit) throws IOException {
        HdfsTextInputSplit hdfsTextInputSplit = (HdfsTextInputSplit) inputSplit;
        org.apache.hadoop.mapred.InputSplit fileSplit = hdfsTextInputSplit.getTextSplit();
        findCurrentPartition(((FileSplit) fileSplit).getPath());
        super.recordReader =
                super.inputFormat.getRecordReader(fileSplit, super.hadoopJobConf, Reporter.NULL);
        super.key = new LongWritable();
        super.value = new Text();
        super.currentReadFilePath = ((FileSplit) fileSplit).getPath().toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            String line =
                    new String(
                            ((Text) value).getBytes(),
                            0,
                            ((Text) value).getLength(),
                            hdfsConfig.getEncoding());
            String[] fields;
            if (StringUtils.isNotBlank(hdfsConfig.getFieldDelimiter())) {
                fields =
                        StringUtils.splitByWholeSeparatorPreserveAllTokens(
                                line, hdfsConfig.getFieldDelimiter());
            } else {
                fields = StringUtils.splitPreserveAllTokens(line, hdfsConfig.getFieldDelimiter());
            }
            List<FieldConfig> fieldConfList = hdfsConfig.getColumn();
            GenericRowData genericRowData;
            if (fieldConfList.size() == 1
                    && ConstantValue.STAR_SYMBOL.equals(fieldConfList.get(0).getName())) {
                genericRowData = new GenericRowData(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    genericRowData.setField(i, fields[i]);
                }
            } else {
                genericRowData = new GenericRowData(fieldConfList.size());
                for (int i = 0; i < fieldConfList.size(); i++) {
                    FieldConfig fieldConfig = fieldConfList.get(i);
                    Object value = null;
                    if (fieldConfig.getValue() != null) {
                        value = fieldConfig.getValue();
                    } else if (fieldConfig.getIndex() != null
                            && fieldConfig.getIndex() < fields.length) {
                        String strVal = fields[fieldConfig.getIndex()];
                        if (!HdfsUtil.NULL_VALUE.equals(strVal)) {
                            value = strVal;
                        }
                    }

                    genericRowData.setField(i, value);
                }
            }
            return rowConverter.toInternal(genericRowData);
        } catch (Exception e) {
            log.error(ExceptionUtil.getErrorMessage(e));
            throw new ReadRecordException("", e, 0, rowData);
        }
    }
}
