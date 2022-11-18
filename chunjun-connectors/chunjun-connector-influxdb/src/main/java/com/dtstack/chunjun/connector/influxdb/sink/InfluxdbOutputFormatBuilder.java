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

package com.dtstack.chunjun.connector.influxdb.sink;

import com.dtstack.chunjun.connector.influxdb.config.InfluxdbSinkConfig;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.util.CollectionUtil;

public class InfluxdbOutputFormatBuilder extends BaseRichOutputFormatBuilder<InfluxdbOutputFormat> {

    public InfluxdbOutputFormatBuilder() {
        super(new InfluxdbOutputFormat());
    }

    public void setInfluxdbConfig(InfluxdbSinkConfig config) {
        super.setConfig(config);
        format.setConfig(config);
    }

    @Override
    protected void checkFormat() {
        InfluxdbSinkConfig sinkConfig = format.getSinkConfig();
        StringBuilder sb = new StringBuilder(256);
        if (CollectionUtil.isNullOrEmpty(sinkConfig.getUrl())) sb.append("No url supplied;\n");
        if (!"insert".equalsIgnoreCase(sinkConfig.getWriteMode().getMode()))
            sb.append("Only support insert write mode;\n");
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public void setDatabase(String database) {
        format.setDatabase(database);
    }

    public void setMeasurement(String measurement) {
        format.setMeasurement(measurement);
    }

    public void setEnableBatch(boolean enableBatch) {
        format.setEnableBatch(enableBatch);
    }
}
