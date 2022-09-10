/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.iceberg.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.iceberg.conf.IcebergReaderConf;
import com.dtstack.chunjun.connector.iceberg.converter.IcebergColumnConverter;
import com.dtstack.chunjun.connector.iceberg.converter.IcebergRawTypeConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * @company: www.dtstack.com
 * @author: shifang
 * @create: 2019/7/4
 */
public class IcebergSourceFactory extends SourceFactory {

    private final IcebergReaderConf icebergConf;

    public IcebergSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        icebergConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getReader().getParameter()),
                        IcebergReaderConf.class);
        icebergConf.setColumn(config.getReader().getFieldList());
        super.initCommonConf(icebergConf);
    }

    @Override
    public DataStream<RowData> createSource() {
        IcebergInputFormatBuilder builder = new IcebergInputFormatBuilder();
        builder.setIcebergConf(icebergConf);
        // 初始化 hadoop conf
        Configuration conf =
                FileSystemUtil.getConfiguration(
                        icebergConf.getHadoopConfig(), icebergConf.getDefaultFS());

        TableLoader tableLoader = TableLoader.fromHadoopTable(icebergConf.getPath(), conf);
        FlinkInputFormat flinkInputFormat =
                FlinkSource.forRowData().env(env).tableLoader(tableLoader).buildFormat();
        builder.setInput(flinkInputFormat);

        AbstractRowConverter rowConverter;
        //        if (useAbstractBaseColumn) {
        rowConverter = new IcebergColumnConverter(icebergConf.getColumn());
        //        } else {
        //            checkConstant(icebergConf);
        //            final RowType rowType =
        //                    TableUtil.createRowType(icebergConf.getColumn(),
        // getRawTypeConverter());
        //            rowConverter = new StreamRowConverter(rowType);
        //        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);

        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return IcebergRawTypeConverter::apply;
    }
}
