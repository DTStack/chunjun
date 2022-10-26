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
package com.dtstack.chunjun.conf;

import com.dtstack.chunjun.cdc.CdcConf;
import com.dtstack.chunjun.mapping.MappingConf;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Date: 2021/01/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class JobConf implements Serializable {
    private static final long serialVersionUID = 1L;

    private LinkedList<ContentConf> content;
    private SettingConf setting = new SettingConf();

    public OperatorConf getReader() {
        return content.get(0).getReader();
    }

    public OperatorConf getWriter() {
        return content.get(0).getWriter();
    }

    public CdcConf getCdcConf() {
        return content.get(0).getRestoration();
    }

    public MappingConf getNameMapping() {
        return content.get(0).getNameMapping();
    }

    public TransformerConf getTransformer() {
        return content.get(0).getTransformer();
    }

    public LinkedList<ContentConf> getContent() {
        return content;
    }

    public void setContent(LinkedList<ContentConf> content) {
        this.content = content;
    }

    public SettingConf getSetting() {
        return setting;
    }

    public void setSetting(SettingConf setting) {
        this.setting = setting;
    }

    @Override
    public String toString() {
        return "JobConf{" + "content=" + content + ", setting=" + setting + '}';
    }
}
