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
package com.dtstack.flinkx.restapi.inputformat;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restapi.common.handler.DataHandler;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiInputFormatBuilder extends BaseRichInputFormatBuilder {
    protected RestapiInputFormat format;


    public RestapiInputFormatBuilder() {
        super.format = format = new RestapiInputFormat();
    }

    public void setHttpRestConfig(HttpRestConfig httpRestConfig) {
        this.format.setHttpRestConfig(httpRestConfig);
    }

    public void setIntervalTime(long intervalTime) {
        this.format.setIntervalTime(intervalTime);
    }

    public void setMetaColumns(List<MetaColumn> metaColumns) {
        this.format.setMetaColumns(metaColumns);
    }

    public void setHandlers(List<DataHandler> handlers) {
        this.format.setHandlers(handlers);
    }

    @Override
    protected void checkFormat() {
        if(StringUtils.isBlank(format.httpRestConfig.getUrl())){
            throw new IllegalArgumentException("缺少url");
        }
        if (StringUtils.isBlank(format.httpRestConfig.getType())) {
            throw new IllegalArgumentException("缺少method");
        }
    }
}
