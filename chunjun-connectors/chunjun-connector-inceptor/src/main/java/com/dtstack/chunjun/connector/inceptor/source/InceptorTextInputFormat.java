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

package com.dtstack.chunjun.connector.inceptor.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.inceptor.inputSplit.InceptorTextInputSplit;
import com.dtstack.chunjun.connector.inceptor.util.InceptorUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;

public class InceptorTextInputFormat extends BaseInceptorFileInputFormat {

    @Override
    public InputSplit[] createInceptorSplit(int minNumSplits) throws IOException {
        super.initHadoopJobConf();
        org.apache.hadoop.mapred.FileInputFormat.setInputPathFilter(
                jobConf, InceptorPathFilter.class);

        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, inceptorFileConf.getPath());
        TextInputFormat inputFormat = new TextInputFormat();

        // 是否在MapReduce中递归遍历Input目录
        jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        inputFormat.configure(jobConf);
        org.apache.hadoop.mapred.InputSplit[] splits = inputFormat.getSplits(jobConf, minNumSplits);

        if (splits != null) {
            InceptorTextInputSplit[] hdfsTextInputSplits =
                    new InceptorTextInputSplit[splits.length];
            for (int i = 0; i < splits.length; ++i) {
                hdfsTextInputSplits[i] = new InceptorTextInputSplit(splits[i], i);
            }
            return hdfsTextInputSplits;
        }
        return null;
    }

    @Override
    public InputFormat createInputFormat() {
        return new TextInputFormat();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        ugi.doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            openHdfsTextReader(inputSplit);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        return null;
                    }
                });
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            String line =
                    new String(
                            ((Text) value).getBytes(),
                            0,
                            ((Text) value).getLength(),
                            inceptorFileConf.getEncoding());
            String[] fields =
                    StringUtils.splitPreserveAllTokens(line, inceptorFileConf.getFieldDelimiter());

            List<FieldConf> fieldConfList = inceptorFileConf.getColumn();
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
                    FieldConf fieldConf = fieldConfList.get(i);
                    Object value = null;
                    if (fieldConf.getValue() != null) {
                        value = fieldConf.getValue();
                    } else if (fieldConf.getIndex() != null
                            && fieldConf.getIndex() < fields.length) {
                        String strVal = fields[fieldConf.getIndex()];
                        if (!InceptorUtil.NULL_VALUE.equals(strVal)) {
                            value = strVal;
                        }
                    }
                    genericRowData.setField(i, value);
                }
            }
            return rowConverter.toInternal(genericRowData);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    private void openHdfsTextReader(InputSplit inputSplit) throws IOException {
        InceptorTextInputSplit hdfsTextInputSplit = (InceptorTextInputSplit) inputSplit;
        org.apache.hadoop.mapred.InputSplit fileSplit = hdfsTextInputSplit.getTextSplit();
        findCurrentPartition(((FileSplit) fileSplit).getPath());
        super.recordReader = super.inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
        super.key = new LongWritable();
        super.value = new Text();
    }
}
