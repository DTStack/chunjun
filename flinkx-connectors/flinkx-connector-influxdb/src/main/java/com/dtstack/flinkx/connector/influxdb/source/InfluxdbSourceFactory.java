/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.flinkx.connector.influxdb.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.influxdb.conf.InfluxdbConfig;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbColumnConverter;
import com.dtstack.flinkx.connector.influxdb.converter.InfluxdbRowTypeConverter;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;

import com.dtstack.flinkx.util.JsonUtil;

import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/8
 */
public class InfluxdbSourceFactory extends SourceFactory {

    private InfluxdbConfig config;
    public InfluxdbSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        Map<String, Object> parameter = syncConf.getJob().getReader().getParameter();
        this.config = JsonUtil.toObject(JsonUtil.toJson(parameter), InfluxdbConfig.class);
        super.initFlinkxCommonConf(config);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InfluxdbRowTypeConverter::apply;
    }

  @Override
  public DataStream<RowData> createSource() {
    InfluxdbInputFormatBuilder builder = new InfluxdbInputFormatBuilder();
    builder.setInfluxdbConfig(config);
    final RowType rowType = TableUtil.createRowType(config.getColumn(), getRawTypeConverter());
    AbstractRowConverter rowConverter;
    if (useAbstractBaseColumn) {
      rowConverter = new InfluxdbColumnConverter(rowType);
      // } else {
      // TODO add InfluxdbRowConverter.
      // }
      builder.setRowConverter(rowConverter);
    }
    return createInput(builder.finish());
  }
}
