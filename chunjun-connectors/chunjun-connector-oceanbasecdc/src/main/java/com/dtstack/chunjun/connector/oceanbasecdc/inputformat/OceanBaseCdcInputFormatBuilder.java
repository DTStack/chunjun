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

package com.dtstack.chunjun.connector.oceanbasecdc.inputformat;

import com.dtstack.chunjun.connector.oceanbasecdc.conf.OceanBaseCdcConf;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import com.oceanbase.clogproxy.client.config.ObReaderConfig;

@SuppressWarnings("rawtypes")
public class OceanBaseCdcInputFormatBuilder
        extends BaseRichInputFormatBuilder<OceanBaseCdcInputFormat> {

    public OceanBaseCdcInputFormatBuilder() {
        super(new OceanBaseCdcInputFormat());
    }

    public void setOceanBaseCdcConf(OceanBaseCdcConf cdcConf) {
        super.setConfig(cdcConf);
        this.format.setCdcConf(cdcConf);
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.setRowConverter(rowConverter, false);
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter, boolean useAbstractColumn) {
        this.format.setRowConverter(rowConverter);
        format.setUseAbstractColumn(useAbstractColumn);
    }

    @Override
    protected void checkFormat() {
        ObReaderConfig obReaderConfig = format.getCdcConf().getObReaderConfig();
        if (!obReaderConfig.valid()) {
            throw new IllegalArgumentException("invalid ObReaderConfig");
        }
    }
}
