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
package com.dtstack.chunjun.config;

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.mapping.MappingConfig;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.StringJoiner;

public class JobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private LinkedList<ContentConfig> content;
    private SettingConfig setting = new SettingConfig();

    public OperatorConfig getReader() {
        return content.get(0).getReader();
    }

    public OperatorConfig getWriter() {
        return content.get(0).getWriter();
    }

    public CdcConfig getCdcConf() {
        return content.get(0).getRestoration();
    }

    public MappingConfig getNameMapping() {
        return content.get(0).getNameMapping();
    }

    public TransformerConfig getTransformer() {
        return content.get(0).getTransformer();
    }

    public LinkedList<ContentConfig> getContent() {
        return content;
    }

    public void setContent(LinkedList<ContentConfig> content) {
        this.content = content;
    }

    public SettingConfig getSetting() {
        return setting;
    }

    public void setSetting(SettingConfig setting) {
        this.setting = setting;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", JobConfig.class.getSimpleName() + "[", "]")
                .add("content=" + content)
                .add("setting=" + setting)
                .toString();
    }
}
