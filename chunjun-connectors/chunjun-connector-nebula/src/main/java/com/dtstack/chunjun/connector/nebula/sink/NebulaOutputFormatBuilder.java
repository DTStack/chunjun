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

package com.dtstack.chunjun.connector.nebula.sink;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.commons.lang3.StringUtils;

public class NebulaOutputFormatBuilder extends BaseRichOutputFormatBuilder<NebulaOutputFormat> {

    public NebulaOutputFormatBuilder(NebulaOutputFormat outputFormat) {
        super(outputFormat);
        this.format = outputFormat;
    }

    public void setNebulaConfig(NebulaConfig nebulaConfig) {
        super.setConfig(nebulaConfig);
        this.format.setNebulaConf(nebulaConfig);
    }

    @Override
    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.format.setRowConverter(rowConverter);
    }

    @Override
    protected void checkFormat() {
        NebulaConfig nebulaConfig = format.getNebulaConf();
        if (StringUtils.isBlank(nebulaConfig.getSpace())) {
            throw new IllegalArgumentException("need graph space name");
        }
        if (nebulaConfig.getGraphdAddresses() == null
                || nebulaConfig.getGraphdAddresses().isEmpty()) {
            throw new IllegalArgumentException("need graph server host:port");
        }

        if (StringUtils.isBlank(nebulaConfig.getEntityName())) {
            throw new IllegalArgumentException("need graph schema name");
        }

        if (nebulaConfig.getSchemaType() == null) {
            throw new IllegalArgumentException("need graph schema type!");
        }

        if (nebulaConfig.getUsername() == null) {
            throw new IllegalArgumentException("need username!");
        }

        if (nebulaConfig.getPassword() == null) {
            throw new IllegalArgumentException("need password!");
        }
    }
}
