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

package com.dtstack.chunjun.connector.starrocks.streamload;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

@Data
public class StarRocksSinkBufferEntity implements Serializable {

    private static final long serialVersionUID = -9221910865925691012L;

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(("yyyyMMdd_HHmmss"));

    private final ArrayList<byte[]> buffer = new ArrayList<>();
    private int batchCount = 0;
    private long batchSize = 0;
    private String label;
    private String database;
    private String table;
    private final List<String> columnList;
    boolean supportDelete;
    String httpHeadColumns;

    public StarRocksSinkBufferEntity(String database, String table, List<String> columnList) {
        this.database = database;
        this.table = table;
        this.columnList = columnList;
        label = initBatchLabel();
    }

    public void addToBuffer(byte[] bts, int count) {
        incBatchCount(count);
        incBatchSize(bts.length);
        buffer.add(bts);
    }

    private void incBatchCount(int count) {
        this.batchCount += count;
    }

    private void incBatchSize(long batchSize) {
        this.batchSize += batchSize;
    }

    public void setSupportDelete(boolean supportDelete, boolean __opAutoProjectionInJson) {
        this.supportDelete = supportDelete;
        if (columnList != null && !__opAutoProjectionInJson) {
            StringJoiner joiner = new StringJoiner(",");
            for (String columnName : columnList) {
                String format = String.format("`%s`", columnName);
                joiner.add(format);
            }
            String res = joiner.toString();
            if (res.length() > 0) {
                res = res + String.format(",%s", "__op");
            }
            httpHeadColumns = res;
        }
    }

    public String getHttpHeadColumns() {
        return httpHeadColumns;
    }

    public synchronized void clear() {
        buffer.clear();
        batchCount = 0;
        batchSize = 0;
        label = initBatchLabel();
    }

    public void reGenerateLabel() {
        label = initBatchLabel();
    }

    public String initBatchLabel() {
        String formatDate = LocalDateTime.now().format(dateTimeFormatter);
        return String.format(
                "chunjun_connector_%s_%s",
                formatDate, UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public String getIdentify() {
        return database + "." + table;
    }
}
