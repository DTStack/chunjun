package com.dtstack.chunjun.connector.nebula.lookup;
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

import com.dtstack.chunjun.connector.nebula.client.NebulaClientFactory;
import com.dtstack.chunjun.connector.nebula.client.NebulaStorageClient;
import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.row.NebulaTableRow;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.FunctionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/10 4:58 下午
 */
public class NebulaAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 1L;

    private final Logger LOG = LoggerFactory.getLogger(NebulaAllTableFunction.class);

    private final NebulaConf nebulaConf;

    private NebulaStorageClient client;

    public NebulaAllTableFunction(
            NebulaConf nebulaConf,
            String[] fieldNames,
            String[] keyNames,
            LookupConfig lookupConf,
            AbstractRowConverter rowConverter) {
        super(fieldNames, keyNames, lookupConf, rowConverter);
        this.nebulaConf = nebulaConf;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        client = NebulaClientFactory.createNebulaStorageClient(nebulaConf);
        client.init();
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        NebulaTableRow nebulaTableRow = null;
        try {
            nebulaTableRow = client.fetchAllData();
            while (nebulaTableRow.hasNext()) {
                HashMap<String, Object> row = new HashMap<>();
                GenericRowData rowData =
                        (GenericRowData) rowConverter.toInternal(nebulaTableRow.next());
                for (int i = 0; i < fieldsName.length; i++) {
                    Object obj = rowData.getField(i);
                    row.put(fieldsName[i].trim(), obj);
                }
                buildCache(row, tmpCache);
            }
        } catch (Exception e) {
            LOG.error("fatch data from nebula error: {}", e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {

        if (client != null) {
            client.close();
        }
    }
}
