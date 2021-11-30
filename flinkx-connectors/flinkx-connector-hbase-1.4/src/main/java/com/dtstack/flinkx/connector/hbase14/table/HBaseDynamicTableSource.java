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
package com.dtstack.flinkx.connector.hbase14.table;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase.HBaseConverter;
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase14.source.HBaseInputFormatBuilder;
import com.dtstack.flinkx.connector.hbase14.table.lookup.HBaseAllTableFunction;
import com.dtstack.flinkx.connector.hbase14.table.lookup.HBaseLruTableFunction;
import com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2021/06/17 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HBaseDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private final HBaseConf hBaseConf;
    private Configuration conf;
    private TableSchema tableSchema;
    private final LookupConf lookupConf;
    private final HBaseTableSchema hbaseSchema;
    protected final String nullStringLiteral;

    public HBaseDynamicTableSource(
            HBaseConf conf,
            TableSchema tableSchema,
            LookupConf lookupConf,
            HBaseTableSchema hbaseSchema,
            String nullStringLiteral) {
        this.hBaseConf = conf;
        this.tableSchema = tableSchema;
        this.lookupConf = lookupConf;
        this.hbaseSchema = hbaseSchema;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);
        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConf field = new FieldConf();
            field.setName(fieldNames[i]);
            field.setType(rowType.getTypeAt(i).asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }
        hBaseConf.setColumn(columnList);
        HBaseInputFormatBuilder builder = new HBaseInputFormatBuilder();
        builder.setColumnMetaInfos(hBaseConf.getColumnMetaInfos());
        builder.setConfig(hBaseConf);
        builder.setEncoding(hBaseConf.getEncoding());
        builder.setHbaseConfig(hBaseConf.getHbaseConfig());
        builder.setTableName(hBaseConf.getTableName());
        AbstractRowConverter rowConverter = new HBaseConverter(rowType);
        builder.setRowConverter(rowConverter);
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                true,
                hBaseConf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new HBaseDynamicTableSource(
                this.hBaseConf, tableSchema, lookupConf, hbaseSchema, nullStringLiteral);
    }

    @Override
    public String asSummaryString() {
        return "HdfsDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "redis only support non-nested look up keys");
            keyNames[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
        }
        setConf();
        hbaseSchema.setTableName(hBaseConf.getTableName());
        if (lookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new HBaseLruTableFunction(
                            conf, lookupConf, hbaseSchema, hBaseConf.getNullMode()),
                    lookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new HBaseAllTableFunction(conf, lookupConf, hbaseSchema, nullStringLiteral),
                lookupConf.getParallelism());
    }

    private void setConf() {
        if (HBaseConfigUtils.isEnableKerberos(hBaseConf.getHbaseConfig())) {
            conf = HBaseConfigUtils.getHadoopConfiguration(hBaseConf.getHbaseConfig());
            String principal = HBaseConfigUtils.getPrincipal(hBaseConf.getHbaseConfig());
            HBaseConfigUtils.fillSyncKerberosConfig(conf, hBaseConf.getHbaseConfig());
            String keytab =
                    HBaseConfigUtils.loadKeyFromConf(
                            hBaseConf.getHbaseConfig(), HBaseConfigUtils.KEY_KEY_TAB);
            String krb5Conf =
                    HBaseConfigUtils.loadKeyFromConf(
                            hBaseConf.getHbaseConfig(),
                            HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF);
            conf.set(HBaseConfigUtils.KEY_HBASE_CLIENT_KEYTAB_FILE, keytab);
            conf.set(HBaseConfigUtils.KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL, principal);
            conf.set(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
        } else {
            conf = HBaseConfigUtils.getConfig(hBaseConf.getHbaseConfig());
        }
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.tableSchema = TableSchemaUtils.projectSchema(tableSchema, projectedFields);
    }
}
