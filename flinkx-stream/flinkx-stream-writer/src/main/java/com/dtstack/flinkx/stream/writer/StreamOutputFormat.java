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

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.List;

/**
 * OutputFormat for stream writer
 *
 * @author jiangbo
 * @Company: www.dtstack.com
 */
public class StreamOutputFormat extends RichOutputFormat {

    protected boolean print;
    protected String writeDelimiter;

    protected List<MetaColumn> metaColumns;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // do nothing
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if (print) {
            System.out.println(String.format("subTaskIndex[%s]:%s", taskNumber, rowToStringWithDelimiter(row, writeDelimiter)));
        }

        // 模拟脏数据的产生
        int n = 0;
        try {
            for (int i = 0; i < row.getArity(); i++) {
                n = i;
                Object val = row.getField(i);
                if (val != null) {
                    StringUtil.string2col(val.toString(), metaColumns.get(i).getType(), null);
                }
            }
        } catch (Exception e) {
            throw new WriteRecordException(recordConvertDetailErrorMessage(n, row), e, n, row);
        }

        if (restoreConfig.isRestore()) {
            formatState.setState(row.getField(restoreConfig.getRestoreColumnIndex()));
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if (print) {
            for (Row row : rows) {
                System.out.println("printInfo: " + rowToStringWithDelimiter(row, writeDelimiter));
            }
            System.out.println("batch size: " + rows.size());
        }

        if (restoreConfig.isRestore()) {
            Row row = rows.get(rows.size() - 1);
            formatState.setState(row.getField(restoreConfig.getRestoreColumnIndex()));
        }
    }

    public String rowToStringWithDelimiter(Row row, String writeDelimiter) {
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
