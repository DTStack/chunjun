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
import com.dtstack.flinkx.util.URLUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * OutputFormat for stream writer
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class StreamOutputFormat extends RichOutputFormat {

    protected boolean print;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        // do nothing
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if (print){
            System.out.println(String.format("subTaskIndex[%s]:%s", taskNumber, row));
        }

        if (restoreConfig.isRestore()){
            formatState.setState(row.getField(restoreConfig.getRestoreColumnIndex()));
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if (print){
            for (Row row : rows) {
                System.out.println(row);
            }
        }

        if (restoreConfig.isRestore()){
            Row row = rows.get(rows.size() - 1);
            formatState.setState(row.getField(restoreConfig.getRestoreColumnIndex()));
        }
    }

    @Override
    public void closeInternal() throws IOException {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String monitors = String.format("%s/jobs/%s", monitorUrl, jobId);
        JsonParser parser = new JsonParser();
        for (int i = 0; i < 10; i++) {
            try{
                String response = URLUtil.get(httpClient, monitors);
                JsonObject obj = parser.parse(response).getAsJsonObject();
                String state = obj.getAsJsonObject("state").getAsString();
                LOG.info("Job state is:{}", state);

                Thread.sleep(500);
            }catch (Exception e){
                LOG.info("{}", e.getMessage());
            }
        }

        httpClient.close();
    }
}
