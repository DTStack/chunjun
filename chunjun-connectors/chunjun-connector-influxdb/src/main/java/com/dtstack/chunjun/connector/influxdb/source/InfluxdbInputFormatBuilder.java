/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.influxdb.source;

import com.dtstack.chunjun.connector.influxdb.config.InfluxdbSourceConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class InfluxdbInputFormatBuilder extends BaseRichInputFormatBuilder<InfluxdbInputFormat> {

    public InfluxdbInputFormatBuilder() {
        super(new InfluxdbInputFormat());
    }

    public void setInfluxdbConfig(InfluxdbSourceConfig config) {
        super.setConfig(config);
        this.format.setConfig(config);
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder();
        InfluxdbSourceConfig config = (InfluxdbSourceConfig) format.getConfig();
        if (config.getParallelism() > 1 && StringUtils.isBlank(config.getSplitPk())) {
            sb.append("splitPk can not be null when speed bigger than 1.");
        }

        if (CollectionUtils.isEmpty(config.getColumn())) {
            sb.append("query column can't be empty.");
        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
