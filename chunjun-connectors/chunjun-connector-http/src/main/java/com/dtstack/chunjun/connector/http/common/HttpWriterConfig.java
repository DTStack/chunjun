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

package com.dtstack.chunjun.connector.http.common;

import com.dtstack.chunjun.config.CommonConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

@SuppressWarnings("all")
public class HttpWriterConfig extends CommonConfig {

    protected String url;

    protected String method;

    protected int delay;

    protected Map<String, String> header;

    protected Map<String, Object> body;

    protected Boolean isAssert = false;

    protected Map<String, Object> params = Maps.newHashMap();

    protected Map<String, String> formatHeader = Maps.newHashMap();

    protected Map<String, Object> formatBody = Maps.newHashMap();

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public Boolean getAssert() {
        return isAssert;
    }

    public void setAssert(Boolean anAssert) {
        isAssert = anAssert;
    }

    public Map<String, String> getHeader() {
        return header;
    }

    public void setHeader(Map<String, String> header) {
        this.header = header;
    }

    public Map<String, Object> getBody() {
        return body;
    }

    public void setBody(Map<String, Object> body) {
        this.body = body;
    }

    protected List<String> columns = Lists.newArrayList();

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public Map<String, String> getFormatHeader() {
        return formatHeader;
    }

    public void setFormatHeader(Map<String, String> formatHeader) {
        this.formatHeader = formatHeader;
    }

    public Map<String, Object> getFormatBody() {
        return formatBody;
    }

    public void setFormatBody(Map<String, Object> formatBody) {
        this.formatBody = formatBody;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
