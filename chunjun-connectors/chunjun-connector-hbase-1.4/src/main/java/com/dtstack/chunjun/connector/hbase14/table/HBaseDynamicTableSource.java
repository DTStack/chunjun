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
package com.dtstack.chunjun.connector.hbase14.table;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.table.BaseHBaseDynamicTableSource;
import com.dtstack.chunjun.connector.hbase.table.lookup.AbstractHBaseAllTableFunction;
import com.dtstack.chunjun.connector.hbase14.converter.HbaseRowConverter;
import com.dtstack.chunjun.connector.hbase14.source.HBaseInputFormatBuilder;
import com.dtstack.chunjun.connector.hbase14.table.lookup.HBaseAllTableFunction;
import com.dtstack.chunjun.connector.hbase14.table.lookup.HBaseLruTableFunction;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.conf.LookupConf;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;

/**
 * Date: 2021/06/17 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HBaseDynamicTableSource extends BaseHBaseDynamicTableSource {

    public HBaseDynamicTableSource(
            HBaseConf conf,
            TableSchema tableSchema,
            LookupConf lookupConf,
            HBaseTableSchema hbaseSchema) {
        super(tableSchema, hbaseSchema, conf, lookupConf);
    }

    @Override
    public DynamicTableSource copy() {
        return new HBaseDynamicTableSource(this.hBaseConf, tableSchema, lookupConf, hbaseSchema);
    }

    @Override
    protected BaseRichInputFormatBuilder getBaseRichInputFormatBuilder() {
        HBaseInputFormatBuilder builder = new HBaseInputFormatBuilder();
        builder.setConfig(hBaseConf);
        builder.setHbaseConfig(hBaseConf.getHbaseConfig());
        builder.sethHBaseConf(hBaseConf);

        AbstractRowConverter rowConverter =
                new HbaseRowConverter(hbaseSchema, hBaseConf.getNullStringLiteral());
        builder.setRowConverter(rowConverter);
        return builder;
    }

    @Override
    protected AbstractLruTableFunction getAbstractLruTableFunction() {
        return new HBaseLruTableFunction(lookupConf, hbaseSchema, hBaseConf);
    }

    @Override
    protected AbstractHBaseAllTableFunction getAbstractAllTableFunction() {
        return new HBaseAllTableFunction(lookupConf, hbaseSchema, hBaseConf);
    }

    @Override
    public String asSummaryString() {
        return "Hbase14DynamicTableSource:";
    }
}
