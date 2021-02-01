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

/**
 * HttpRestConfig
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/28
 */
public class HttpRestConfig implements Serializable {

    /**
     * http协议 https/http
     **/
    private String protocol;

    /**
     * http请求地址
     **/
    private String url;

    /**
     * http请求方式 post/get/...
     **/
    private String requestMode;


    /**
     * 对返回值的处理 text/json
     **/
    private String decode;

    /**
     * decode为json时，指定解析的key
     */
    private String fields;


    /**
     * 请求的间隔时间 单位毫秒
     **/
    private Long intervalTime;

    /**
     * 对返回的json解析出对应的字段
     **/
    private List columns;

    /**
     * 请求的header头
     **/
    private List header;

    /**
     * 请求的param
     **/
    private List param;


    /**
     * 请求的body
     **/
    private List body;

    /**
     * 返回结果的处理策略
     **/
    protected List<Strategy> strategy;

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getRequestMode() {
        return requestMode;
    }

    public void setRequestMode(String requestMode) {
        this.requestMode = requestMode;
    }

    public String getDecode() {
        return decode;
    }

    public void setDecode(String decode) {
        this.decode = decode;
    }

    public Long getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(Long intervalTime) {
        this.intervalTime = intervalTime;
    }

    public List getColumns() {
        return columns;
    }

    public void setColumns(List columns) {
        this.columns = columns;
    }

    public List getHeader() {
        return header;
    }

    public void setHeader(List header) {
        this.header = header;
    }

    public List getParam() {
        return param;
    }

    public void setParam(List param) {
        this.param = param;
    }


    public List<Strategy> getStrategy() {
        return strategy;
    }

    public void setStrategy(List<Strategy> strategy) {
        this.strategy = strategy;
    }

    public List getBody() {
        return body;
    }

    public void setBody(List body) {
        this.body = body;
    }

    public  boolean isJsonDecode(){
        return  getDecode().equalsIgnoreCase("json");
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.fields = fields;
    }
}
