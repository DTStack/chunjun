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

package com.dtstack.flinkx.stream.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class StreamInputFormat extends BaseRichInputFormat {

    protected static final long serialVersionUID = 1L;

    private long recordRead = 0;

    protected List<Long> sliceRecordCount;

    protected List<MetaColumn> columns;

    private long channelRecordNum;

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        if(CollectionUtils.isNotEmpty(sliceRecordCount) && sliceRecordCount.size() > inputSplit.getSplitNumber()){
            channelRecordNum = sliceRecordCount.get(inputSplit.getSplitNumber());
        }

        LOG.info("The record number of channel:[{}] is [{}]", inputSplit.getSplitNumber(), channelRecordNum);
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        return MockDataUtil.getMockRow(columns);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return ++recordRead > channelRecordNum && channelRecordNum > 0;
    }

    @Override
    protected void closeInternal() throws IOException {
        recordRead = 0;
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i,minNumSplits);
        }

        return inputSplits;
    }
}
