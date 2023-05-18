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

package com.dtstack.chunjun.connector.kudu.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduSourceConfig;
import com.dtstack.chunjun.connector.kudu.converter.KuduRawTypeMapper;
import com.dtstack.chunjun.connector.kudu.converter.KuduSyncConverter;
import com.dtstack.chunjun.connector.kudu.util.KuduUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class KuduInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = -2213920219898440077L;

    private KuduSourceConfig sourceConf;

    private transient KuduClient client;

    private transient KuduScanner scanner;

    private transient RowResultIterator iterator;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        log.info("execute createInputSplits,minNumSplits:{}", minNumSplits);
        List<KuduScanToken> scanTokens = KuduUtil.getKuduScanToken(sourceConf);
        KuduInputSplit[] inputSplits = new KuduInputSplit[scanTokens.size()];
        for (int i = 0; i < scanTokens.size(); i++) {
            inputSplits[i] = new KuduInputSplit(scanTokens.get(i).serialize(), i);
        }

        return inputSplits;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        client = KuduUtil.getKuduClient(sourceConf);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

        log.info(
                "Execute openInternal: splitNumber = {}, indexOfSubtask  = {}",
                inputSplit.getSplitNumber(),
                indexOfSubTask);

        KuduInputSplit kuduTableSplit = (KuduInputSplit) inputSplit;
        scanner = KuduScanToken.deserializeIntoScanner(kuduTableSplit.getToken(), client);

        if (rowConverter == null) {
            List<String> columnNames = new ArrayList<>();
            List<FieldConfig> fieldConfList = sourceConf.getColumn();
            fieldConfList.forEach(field -> columnNames.add(field.getName()));
            RowType rowType = TableUtil.createRowType(fieldConfList, KuduRawTypeMapper::apply);
            setRowConverter(new KuduSyncConverter(rowType, columnNames));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            RowResult result = iterator.next();
            rowData = rowConverter.toInternal(result);
        } catch (Exception e) {
            throw new ReadRecordException("Kudu next record error!", e, -1, rowData);
        }
        return rowData;
    }

    @Override
    protected void closeInternal() {

        log.info("closeInternal: closing input format.");

        if (scanner != null) {
            try {
                scanner.close();
                scanner = null;
            } catch (KuduException e) {
                log.warn("Kudu Scanner close failed.", e);
            }
        }
    }

    @Override
    public void closeInputFormat() {
        super.closeInputFormat();

        try {
            if (client != null) {
                client.close();
                client = null;
            }
        } catch (KuduException e) {
            log.error("Close kudu client failed.", e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (iterator != null && iterator.hasNext()) {
            return false;
        } else if (scanner.hasMoreRows()) {
            iterator = scanner.nextRows();
            return reachedEnd();
        } else {
            return true;
        }
    }

    public void setSourceConf(KuduSourceConfig sourceConf) {
        this.sourceConf = sourceConf;
    }

    public KuduSourceConfig getSourceConf() {
        return sourceConf;
    }

    public AbstractRowConverter getCdcRowConverter() {
        return rowConverter;
    }
}
