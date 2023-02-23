package com.dtstack.chunjun.connector.selectdbcloud.rest.http;

import org.apache.flink.util.Preconditions;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class HttpPostBuilder {
    String url;
    Map<String, String> header;
    HttpEntity httpEntity;

    public HttpPostBuilder() {
        header = new HashMap<>();
    }

    public HttpPostBuilder setUrl(String url) {
        this.url = url;
        return this;
    }

    public HttpPostBuilder addCommonHeader() {
        header.put(HttpHeaders.EXPECT, "100-continue");
        return this;
    }

    public HttpPostBuilder baseAuth(String user, String password) {
        final String authInfo = user + ":" + password;
        header.put(
                HttpHeaders.AUTHORIZATION,
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(authInfo.getBytes(StandardCharsets.UTF_8)));
        return this;
    }

    public HttpPostBuilder setEntity(HttpEntity httpEntity) {
        this.httpEntity = httpEntity;
        return this;
    }

    public HttpPost build() {
        Preconditions.checkNotNull(url);
        Preconditions.checkNotNull(httpEntity);
        HttpPost put = new HttpPost(url);
        header.forEach(put::setHeader);
        put.setEntity(httpEntity);
        return put;
    }
}
