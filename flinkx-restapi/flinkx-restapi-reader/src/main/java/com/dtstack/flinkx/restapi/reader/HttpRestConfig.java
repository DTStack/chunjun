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
package com.dtstack.flinkx.restapi.reader;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * HttpRestConfig
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/28
 */
public class HttpRestConfig implements Serializable {
    private String url;
    private String type;
    private Long intervalTime;
    private List columns;
    private String format;
    private Map<String, Map<String,String>> header;
    private Map<String, Map<String,String>> body;
    private Map<String, Map<String,String>> param;
    protected List handlers;


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List getColumns() {
        return columns;
    }

    public void setColumns(List columns) {
        this.columns = columns;
    }

    public Map<String, Map<String, String>> getHeader() {
        return header;
    }

    public void setHeader(Map<String, Map<String, String>> header) {
        this.header = header;
    }

    public Map<String, Map<String, String>> getBody() {
        return body;
    }

    public void setBody(Map<String, Map<String, String>> body) {
        this.body = body;
    }

    public Map<String, Map<String, String>> getParam() {
        return param;
    }

    public void setParam(Map<String, Map<String, String>> param) {
        this.param = param;
    }

    public Long getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(Long intervalTime) {
        this.intervalTime = intervalTime;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List getHandlers() {
        return handlers;
    }

    public void setHandlers(List handlers) {
        this.handlers = handlers;
    }
}
