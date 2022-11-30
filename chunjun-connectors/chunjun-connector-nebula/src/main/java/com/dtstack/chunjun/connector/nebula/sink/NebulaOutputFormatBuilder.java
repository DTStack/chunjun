package com.dtstack.chunjun.connector.nebula.sink;
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

import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.commons.lang3.StringUtils;

/**
 * @author: gaoasi
 * @create: 2022/09/22
 */
public class NebulaOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private NebulaOutputFormat outputFormat;

    public NebulaOutputFormatBuilder(NebulaOutputFormat outputFormat) {
        super(outputFormat);
        super.format = this.outputFormat = outputFormat;
    }

    public void setNebulaConfig(NebulaConf nebulaConf) {
        super.setConfig(nebulaConf);
        this.outputFormat.setNebulaConf(nebulaConf);
    }

    @Override
    public void setRowConverter(AbstractRowConverter rowConverter) {
        this.outputFormat.setRowConverter(rowConverter);
    }

    @Override
    protected void checkFormat() {
        NebulaConf nebulaConf = outputFormat.getNebulaConf();
        if (StringUtils.isBlank(nebulaConf.getSpace())) {
            throw new IllegalArgumentException("need graph space name");
        }
        if (nebulaConf.getGraphdAddresses() == null || nebulaConf.getGraphdAddresses().isEmpty()) {
            throw new IllegalArgumentException("need graph server host:port");
        }

        if (StringUtils.isBlank(nebulaConf.getEntityName())) {
            throw new IllegalArgumentException("need graph schema name");
        }

        if (nebulaConf.getSchemaType() == null) {
            throw new IllegalArgumentException("need graph schema type!");
        }

        if (nebulaConf.getUsername() == null) {
            throw new IllegalArgumentException("need username!");
        }

        if (nebulaConf.getPassword() == null) {
            throw new IllegalArgumentException("need password!");
        }
    }
}
