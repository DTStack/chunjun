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

package com.dtstack.flinkx.connector.kudu.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.kudu.conf.KuduSourceConf;
import com.dtstack.flinkx.connector.kudu.converter.KuduColumnConverter;
import com.dtstack.flinkx.connector.kudu.converter.KuduRawTypeConverter;
import com.dtstack.flinkx.connector.kudu.util.KuduUtil;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/9 星期三
 */
public class KuduInputFormat extends BaseRichInputFormat {

    private KuduSourceConf sourceConf;

    private transient KuduClient client;

    private transient KuduScanner scanner;

    private transient RowResultIterator iterator;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        LOG.info("execute createInputSplits,minNumSplits:{}", minNumSplits);
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

        LOG.info(
                "Execute openInternal: splitNumber = {}, indexOfSubtask  = {}",
                inputSplit.getSplitNumber(),
                indexOfSubTask);

        KuduInputSplit kuduTableSplit = (KuduInputSplit) inputSplit;
        scanner = KuduScanToken.deserializeIntoScanner(kuduTableSplit.getToken(), client);

        List<String> columnNames = new ArrayList<>();
        List<FieldConf> fieldConfList = sourceConf.getColumn();
        fieldConfList.forEach(field -> columnNames.add(field.getName()));
        RowType rowType = TableUtil.createRowType(fieldConfList, KuduRawTypeConverter::apply);

        setRowConverter(
                rowConverter == null
                        ? new KuduColumnConverter(rowType, columnNames)
                        : rowConverter);
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

        LOG.info("closeInternal: closing input format.");

        if (scanner != null) {
            try {
                scanner.close();
                scanner = null;
            } catch (KuduException e) {
                LOG.warn("Kudu Scanner close failed.", e);
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
            LOG.error("Close kudu client failed.", e);
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

    public void setSourceConf(KuduSourceConf sourceConf) {
        this.sourceConf = sourceConf;
    }

    public KuduSourceConf getSourceConf() {
        return sourceConf;
    }
}
