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

package com.dtstack.chunjun.connector.emqx.sink;

import com.dtstack.chunjun.connector.emqx.conf.EmqxConf;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.commons.lang3.StringUtils;

/**
 * @author chuixue
 * @create 2021-06-04 09:55
 * @description
 */
public class EmqxOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    protected EmqxOutputFormat format;

    public EmqxOutputFormatBuilder() {
        super.format = format = new EmqxOutputFormat();
    }

    public void setEmqxConf(EmqxConf emqxConf) {
        super.setConfig(emqxConf);
        format.setEmqxConf(emqxConf);
    }

    @Override
    protected void checkFormat() {
        EmqxConf emqxConf = format.getEmqxConf();
        if (StringUtils.isBlank(emqxConf.getBroker())) {
            throw new IllegalArgumentException("emqx broker cannot be blank");
        }
        if (StringUtils.isBlank(emqxConf.getTopic())) {
            throw new IllegalArgumentException("emqx topic cannot be blank");
        }
    }
}
