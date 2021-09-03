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
package com.dtstack.flinkx.connector.hdfs.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hdfs.conf.HdfsConf;
import com.dtstack.flinkx.connector.hdfs.converter.HdfsOrcRowConverter;
import com.dtstack.flinkx.connector.hdfs.converter.HdfsParquetRowConverter;
import com.dtstack.flinkx.connector.hdfs.converter.HdfsTextRowConverter;
import com.dtstack.flinkx.connector.hdfs.enums.FileType;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2021/06/17 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsDynamicTableSource implements ScanTableSource {

    private final HdfsConf hdfsConf;
    private final TableSchema tableSchema;

    public HdfsDynamicTableSource(HdfsConf hdfsConf, TableSchema tableSchema) {
        this.hdfsConf = hdfsConf;
        this.tableSchema = tableSchema;
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
        hdfsConf.setColumn(columnList);
        HdfsInputFormatBuilder builder = new HdfsInputFormatBuilder(hdfsConf.getFileType());
        builder.setHdfsConf(hdfsConf);
        AbstractRowConverter rowConverter;
        switch (FileType.getByName(hdfsConf.getFileType())) {
            case ORC:
                rowConverter = new HdfsOrcRowConverter(rowType);
                break;
            case PARQUET:
                rowConverter = new HdfsParquetRowConverter(rowType);
                break;
            default:
                rowConverter = new HdfsTextRowConverter(rowType);
        }
        builder.setRowConverter(rowConverter);
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                hdfsConf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new HdfsDynamicTableSource(this.hdfsConf, this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HdfsDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
