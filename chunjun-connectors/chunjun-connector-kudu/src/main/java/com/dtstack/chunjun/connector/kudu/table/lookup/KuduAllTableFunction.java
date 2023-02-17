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

package com.dtstack.chunjun.connector.kudu.table.lookup;

import com.dtstack.chunjun.connector.kudu.config.KuduCommonConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduLookupConfig;
import com.dtstack.chunjun.connector.kudu.util.KuduUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.util.ThreadUtil;

import org.apache.flink.table.data.GenericRowData;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class KuduAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 7111000413256579328L;

    private final KuduLookupConfig kuduLookupConfig;

    private KuduClient client;

    private KuduTable table;

    public KuduAllTableFunction(
            KuduLookupConfig kuduLookupConfig,
            AbstractRowConverter<?, ?, ?, ?> rowConverter,
            String[] fieldNames,
            String[] keyNames) {
        super(fieldNames, keyNames, kuduLookupConfig, rowConverter);
        this.kuduLookupConfig = kuduLookupConfig;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        KuduScanner scanner = getKuduScannerWithRetry(kuduLookupConfig);
        // load data from table
        if (Objects.isNull(scanner)) {
            throw new NullPointerException("kudu scanner is null");
        }

        while (scanner.hasMoreRows()) {
            try {
                RowResultIterator results = scanner.nextRows();

                if (Objects.isNull(results)) {
                    break;
                }

                while (results.hasNext()) {
                    RowResult result = results.next();
                    GenericRowData rowData = (GenericRowData) rowConverter.toInternalLookup(result);
                    Map<String, Object> oneRow = Maps.newHashMap();
                    for (int i = 0; i < fieldsName.length; i++) {
                        Object object = rowData.getField(i);
                        oneRow.put(fieldsName[i].trim(), object);
                    }
                    buildCache(oneRow, tmpCache);
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }

        closeScanner(scanner);
    }

    private void closeScanner(KuduScanner scanner) {
        try {
            scanner.close();
        } catch (KuduException ke) {
            log.error("", ke);
        }
    }

    private KuduScanner getKuduScannerWithRetry(KuduLookupConfig kuduLookupConfig) {
        KuduCommonConfig commonConfig = kuduLookupConfig.getCommonConfig();
        String connInfo =
                "kuduMasters:"
                        + commonConfig.getMasters()
                        + ";tableName:"
                        + kuduLookupConfig.getTableName();
        for (int i = 0; i < 3; i++) {
            try {
                if (Objects.isNull(client)) {
                    String tableName = kuduLookupConfig.getTableName();
                    client = KuduUtil.getKuduClient(commonConfig);
                    if (!client.tableExists(tableName)) {
                        throw new IllegalArgumentException(
                                "Table Open Failed , please check table exists");
                    }
                    table = client.openTable(tableName);
                }

                KuduScanner.KuduScannerBuilder tokenBuilder = client.newScannerBuilder(table);
                return buildScanner(tokenBuilder, kuduLookupConfig);
            } catch (Exception e) {
                log.error("connect kudu is error:" + e.getMessage());
                log.error("connInfo\n " + connInfo);
                ThreadUtil.sleepMilliseconds(5);
            }
        }
        throw new RuntimeException("Get kudu connect failed! Current Conn Info \n" + connInfo);
    }

    /**
     * @param scannerBuilder 创建AsyncKuduScanner对象
     * @param kuduLookupConfig Kudu lookup configuration
     * @return kudu scanner
     */
    private KuduScanner buildScanner(
            KuduScanner.KuduScannerBuilder scannerBuilder, KuduLookupConfig kuduLookupConfig) {
        Integer batchSizeBytes = kuduLookupConfig.getBatchSizeBytes();
        Boolean isFaultTolerant = kuduLookupConfig.getIsFaultTolerant();

        return scannerBuilder
                .batchSizeBytes(batchSizeBytes)
                .setFaultTolerant(isFaultTolerant)
                .setProjectedColumnNames(Arrays.asList(fieldsName))
                .build();
    }

    @Override
    public void close() {
        // 公用一个client  如果每次刷新间隔时间较长可以每次获取一个
        try {
            super.close();

            if (Objects.nonNull(client)) {
                client.close();
            }
        } catch (Exception e) {
            log.error("Error while closing client.", e);
        }
    }
}
