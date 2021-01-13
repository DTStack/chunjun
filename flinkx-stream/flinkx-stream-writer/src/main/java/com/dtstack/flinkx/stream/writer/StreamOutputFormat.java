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

package com.dtstack.flinkx.stream.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restore.FormatState;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.util.List;

/**
 * OutputFormat for stream writer
 *
 * @author jiangbo
 * @Company: www.dtstack.com
 */
public class StreamOutputFormat extends BaseRichOutputFormat {

    protected boolean print;
    protected String writeDelimiter;

    protected List<MetaColumn> metaColumns;

    protected Row lastRow;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        // do nothing
    }

    @Override
    protected void writeSingleRecordInternal(Row row) {
        if (print) {
            LOG.info("subTaskIndex[{}]:{}", taskNumber, rowToStringWithDelimiter(row, writeDelimiter));
        }
        lastRow = row;
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        if (print) {
            for (Row row : rows) {
                LOG.info(rowToStringWithDelimiter(row, writeDelimiter));
            }
        }
        if(rows.size() > 1){
            lastRow = rows.get(rows.size() - 1);
        }
    }

    public FormatState getFormatState(){
        if(lastRow != null){
            LOG.info("subTaskIndex[{}]:{}", taskNumber, rowToStringWithDelimiter(lastRow, writeDelimiter));
        }
        return super.getFormatState();
    }

    public String rowToStringWithDelimiter(Row row, String writeDelimiter) {
        if(row == null){
            return "";
        }else{
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.getArity(); i++) {
                if (i > 0) {
                    sb.append(writeDelimiter);
                }
                sb.append(StringUtils.arrayAwareToString(row.getField(i)));
            }
            return sb.toString();
        }
    }
}
