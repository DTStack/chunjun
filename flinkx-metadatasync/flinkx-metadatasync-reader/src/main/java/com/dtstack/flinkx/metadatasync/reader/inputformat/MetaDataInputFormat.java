/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadatasync.reader.inputformat;

import com.dtstack.flinkx.rdb.util.DBUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/5
 * @description :
 */
public class MetaDataInputFormat extends AbstractMetaInputFormat {

    protected boolean hasNext = true;

    protected Map<String, Object> currentMessage;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        currentQueryTable = ((MetadataInputSplit) inputSplit).getTable();
        hasNext = true;
        currentMessage = getMetaData(inputSplit);
    }


    /**
     * 如果程序正常执行，那么传递正常的数据，程序出现异常，则传递异常信息
     */
    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        row = new Row(1);
        if (errorMessage.isEmpty()) {
            row.setField(0, objectMapper.writeValueAsString(currentMessage));
        } else {
            row.setField(0, objectMapper.writeValueAsString(errorMessage));
        }
        hasNext = false;
        tableColumn.clear();
        partitionColumn.clear();
        errorMessage.clear();

        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();
        DBUtil.closeDBResources(resultSet, statement, connection, true);
    }
}
