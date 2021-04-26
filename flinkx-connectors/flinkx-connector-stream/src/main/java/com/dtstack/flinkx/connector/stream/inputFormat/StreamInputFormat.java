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

package com.dtstack.flinkx.connector.stream.inputFormat;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.stream.conf.StreamConf;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import org.apache.commons.collections.CollectionUtils;

/**
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class StreamInputFormat extends BaseRichInputFormat {
    protected static final long serialVersionUID = 1L;

    private StreamConf streamConf;

    private long recordRead = 0;
    private long channelRecordNum;

    private DataGenerator<?>[] fieldGenerators;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i,minNumSplits);
        }

        return inputSplits;
    }

    @Override
    public void openInternal(InputSplit inputSplit) {
//        try {
//            FieldConf[] fieldNames = streamConf.getColumn().toArray(new FieldConf[0]);
//            for (int i = 0; i < fieldGenerators.length; i++) {
//                fieldGenerators[i].open(fieldNames[i].getName(), functionInitializationContext, context);
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

        if (CollectionUtils.isNotEmpty(streamConf.getSliceRecordCount())
                && streamConf.getSliceRecordCount().size() > inputSplit.getSplitNumber()) {
            channelRecordNum = streamConf.getSliceRecordCount().get(inputSplit.getSplitNumber());
        }

        LOG.info("The record number of channel:[{}] is [{}]", inputSplit.getSplitNumber(), channelRecordNum);
    }

    @Override
    @SuppressWarnings("all")
    public RowData nextRecordInternal(RowData rowData) {
        try {
            rowData = rowConverter.toInternal(rowData);
        } catch (Exception e) {
            LOG.error("", e);
        }
        return rowData;
    }

    @Override
    public boolean reachedEnd() {
        return ++recordRead > channelRecordNum && channelRecordNum > 0;
    }

    @Override
    protected void closeInternal() {
        recordRead = 0;
    }

    public StreamConf getStreamConf() {
        return streamConf;
    }

    public void setStreamConf(StreamConf streamConf) {
        this.streamConf = streamConf;
    }

    public void setFieldGenerators(DataGenerator<?>[] fieldGenerators) {
        this.fieldGenerators = fieldGenerators;
    }
}
