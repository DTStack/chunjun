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


package com.dtstack.flinkx.kudu.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.kudu.core.KuduConfig;
import com.dtstack.flinkx.kudu.core.KuduUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/7/31
 */
public class KuduInputFormat extends BaseRichInputFormat {

    protected List<MetaColumn> columns;

    protected KuduConfig kuduConfig;

    protected Map<String,Object> hadoopConfig;

    private transient KuduClient client;

    private transient KuduScanner scanner;

    private transient RowResultIterator iterator;

    @Override
    public void openInputFormat() throws IOException {
        LOG.info("execute openInputFormat");
        super.openInputFormat();

        try {
            client = KuduUtil.getKuduClient(kuduConfig, hadoopConfig);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Get KuduClient error", e);
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("execute openInternal,splitNumber = {}, indexOfSubtask  = {}", inputSplit.getSplitNumber(), indexOfSubTask);
        KuduTableSplit kuduTableSplit = (KuduTableSplit) inputSplit;
        scanner = KuduScanToken.deserializeIntoScanner(kuduTableSplit.getToken(), client);
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        row = new Row(columns.size());
        RowResult rowResult = iterator.next();

        for (int i = 0; i < columns.size(); i++) {
            MetaColumn column = columns.get(i);
            Type type = KuduUtil.getType(column.getType());
            if (column.getValue() != null) {
                row.setField(i, KuduUtil.getValue(column.getValue(), type));
            } else {
                row.setField(i, getValue(type, rowResult, column.getName()));
            }
        }

        LOG.info("nextRecordInternal, numReadCounter = {}", numReadCounter.getLocalValue());
        return row;
    }

    private Object getValue(Type type, RowResult rowResult, String name) {
        Object objValue;

        if (Type.BOOL.equals(type)) {
            objValue = rowResult.getBoolean(name);
        } else if (Type.INT8.equals(type)) {
            objValue = rowResult.getByte(name);
        } else if (Type.INT16.equals(type)) {
            objValue = rowResult.getShort(name);
        } else if (Type.INT32.equals(type)) {
            objValue = rowResult.getInt(name);
        } else if (Type.INT64.equals(type)) {
            objValue = rowResult.getLong(name);
        } else if (Type.FLOAT.equals(type)) {
            objValue = rowResult.getFloat(name);
        } else if (Type.DOUBLE.equals(type)) {
            objValue = rowResult.getDouble(name);
        } else if (Type.DECIMAL.equals(type)) {
            objValue = rowResult.getDecimal(name);
        } else if (Type.BINARY.equals(type)) {
            objValue = rowResult.getBinary(name);
        } else if (Type.UNIXTIME_MICROS.equals(type)) {
            objValue = rowResult.getTimestamp(name);
        } else {
            objValue = rowResult.getString(name);
        }

        return objValue;
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        LOG.info("execute createInputSplits,minNumSplits:{}", minNumSplits);
        List<KuduScanToken> scanTokens = KuduUtil.getKuduScanToken(kuduConfig, columns, kuduConfig.getFilterString(), hadoopConfig);
        KuduTableSplit[] inputSplits = new KuduTableSplit[scanTokens.size()];
        for (int i = 0; i < scanTokens.size(); i++) {
            inputSplits[i] = new KuduTableSplit(scanTokens.get(i).serialize(), i);
        }

        return inputSplits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        LOG.info("execute reachedEnd, indexOfSubtask = {}", indexOfSubTask);
        if (iterator == null || !iterator.hasNext()) {
            return getNextRows();
        }

        return false;
    }

    private boolean getNextRows() throws IOException {
        LOG.info("execute getNextRows, scanner is closed : {}", scanner.isClosed());
        if (scanner.hasMoreRows()) {
            iterator = scanner.nextRows();
        }

        return iterator == null || !iterator.hasNext();
    }

    @Override
    protected void closeInternal() throws IOException {
        LOG.info("execute closeInternal, indexOfSubtask = {}", indexOfSubTask);
        if (scanner != null) {
            scanner.close();
            scanner = null;
        }
    }

    @Override
    public void closeInputFormat() throws IOException {
        super.closeInputFormat();

        if (client != null) {
            client.close();
            client = null;
        }
    }

    public Map<String, Object> getHadoopConfig() {
        return hadoopConfig;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }
}
