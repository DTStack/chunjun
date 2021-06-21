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
package com.dtstack.flinkx.connector.hdfs.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.hdfs.conf.HdfsConf;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.GsonUtil;

/**
 * Date: 2021/06/09
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsSinkFactory extends SinkFactory {

    private final HdfsConf hdfsConf;

    public HdfsSinkFactory(SyncConf config) {
        super(config);
        hdfsConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()),
                        HdfsConf.class);
        hdfsConf.setColumn(config.getWriter().getFieldList());
        super.initFlinkxCommonConf(hdfsConf);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(hdfsConf.getFileType());
        builder.setHdfsConf(hdfsConf);
        AbstractRowConverter converter;
//        if (useAbstractBaseColumn) {
//            converter = new StreamColumnConverter();
//        } else {
//            final RowType rowType =
//                    TableUtil.createRowType(streamConf.getColumn(), getRawTypeConverter());
//            converter = new StreamRowConverter(rowType);
//        }

//        builder.setConverter(converter);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
