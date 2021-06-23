package com.dtstack.flinkx.connector.restapi.common;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.internal.LinkedTreeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RestapiWriterConfig extends FlinkxCommonConf {


    protected String url;

    protected String method;


    protected List<Map<String, String>> header ;

    protected List<Map<String, Object>> body ;

    protected Map<String, Object> params =Maps.newHashMap();

    protected Map<String, String> formatHeader = Maps.newHashMap();

    protected Map<String, Object> formatBody =Maps.newHashMap();

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

    public List<Map<String, String>> getHeader() {
        return header;
    }

    public void setHeader(List<Map<String, String>> header) {
        this.header = header;
    }

    public List<Map<String, Object>> getBody() {
        return body;
    }

    public void setBody(List<Map<String, Object>> body) {
        this.body = body;
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
