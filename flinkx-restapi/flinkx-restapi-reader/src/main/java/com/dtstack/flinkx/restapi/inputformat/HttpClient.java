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
package com.dtstack.flinkx.restapi.inputformat;

import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import com.dtstack.flinkx.restapi.reader.Strategy;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.flink.types.Row;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * httpClient
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/25
 */
public class HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private ScheduledExecutorService scheduledExecutorService;
    private transient CloseableHttpClient httpClient;
    private BlockingQueue<Row> queue;
    private static final String THREAD_NAME = "restApiReader-thread";


    protected HttpRestConfig restConfig;

    private boolean first = true;

    private RestHandler restHandler;

    /**
     * 内部重试次数(返回的httpStatus 不是 200 就进行重试 默认2次)
     **/
    private int requestRetryTime;

    /**
     * json字段信息
     **/
    private String fields;


    /**
     * 原始请求body
     */
    private List<MetaParam> originalBody;

    /**
     * 原始请求param
     */
    private List<MetaParam> originalParam;

    /**
     * 原始请求header
     */
    private List<MetaParam> originalHeader;

    /**
     * 当前请求参数
     */
    private HttpRequestParam currentParam;

    /**
     * 上次请求参数
     */
    private HttpRequestParam prevParam;


    /**
     * 上一次请求的返回值
     */
    private String prevResponse;

    private boolean reachEnd;


    public HttpClient(HttpRestConfig httpRestConfig, String fields, List<MetaParam> originalBody, List<MetaParam> originalParam, List<MetaParam> originalHeader) {
        this.restConfig = httpRestConfig;
        this.fields = fields;
        this.originalHeader = originalHeader;
        this.originalBody = originalBody;
        this.originalParam = originalParam;
        this.currentParam = new HttpRequestParam();
        this.prevParam = new HttpRequestParam();
        this.httpClient = HttpUtil.getHttpClient();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, THREAD_NAME));
        this.restHandler = new DefaultRestHandler();
        this.queue = new LinkedBlockingQueue<Row>();
        this.reachEnd = false;
        this.requestRetryTime = 2;
    }

    public void start() {
        scheduledExecutorService.scheduleWithFixedDelay(
                this::execute,
                0,
                restConfig.getIntervalTime(),
                TimeUnit.MILLISECONDS
        );
    }

    public void execute() {

        if (restConfig.isJsonDecode()) {
            currentParam = restHandler.buildRequestParam(originalBody, originalHeader, prevParam, GsonUtil.GSON.fromJson(prevResponse, Map.class), restConfig, first);
        } else {
            currentParam = restHandler.buildRequestParam(originalBody, originalHeader, prevParam, null, restConfig, first);
        }

        LOG.info("currentParam is {}", currentParam);
        doExecute(com.dtstack.flinkx.restapi.common.ConstantValue.REQUEST_RETRY_TIME);
        first = false;
        requestRetryTime = 2;
    }

    public void doExecute(int retryTime) {

        HttpUriRequest request = HttpUtil.getRequest(restConfig.getRequestMode(), currentParam.getBody(),currentParam.getParam(), currentParam.getHeader(), restConfig.getUrl());
        prevParam = currentParam;

        try {
            CloseableHttpResponse httpResponse = httpClient.execute(request);
            if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                if (retryTime == 0) {
                    queue.put(Row.of("exit job when response status is not 200,the last response status is " + httpResponse.getStatusLine().getStatusCode()));
                } else {
                    doExecute(--requestRetryTime);
                    return;
                }
            }

            String responseValue = EntityUtils.toString(httpResponse.getEntity());
            Strategy strategy;
            if (restConfig.isJsonDecode()) {
                strategy = restHandler.chooseStrategy(restConfig.getStrategy(), GsonUtil.GSON.fromJson(responseValue, Map.class), restConfig, currentParam);
            } else {
                strategy = restHandler.chooseStrategy(restConfig.getStrategy(), null, restConfig, currentParam);
            }

            if (strategy != null) {
                //进行策略的执行
                switch (strategy.getHandle()) {
                    case "retry":
                        doExecute(--retryTime);
                        return;
                    case "stop":
                        reachEnd = true;
                        close();
                        break;
                    default:
                        break;
                }
            }

            if (reachEnd) {
                //如果结束了  需要告诉format 结束了
                ResponseValue value = restHandler.buildData(restConfig.getDecode(), responseValue, fields);
                if (value.isNormal()) {
                    value.setStatus(0);
                }
                queue.put(Row.of(value));
            } else {
                queue.put(Row.of(restHandler.buildData(restConfig.getDecode(), responseValue, fields)));
            }

            prevResponse = responseValue;
        } catch (Throwable e) {
            LOG.warn(ExceptionUtil.getErrorMessage(e));
            doExecute(--retryTime);
        }

        if (reachEnd) {
            close();
        }
    }


    public Row takeEvent() {
        Row row = null;
        try {
            row = queue.poll();
        } catch (Exception e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }

        return row;
    }


    public void close() {
        try {
            HttpUtil.closeClient(httpClient);
            scheduledExecutorService.shutdown();
        } catch (Exception e) {
            LOG.warn("close resource error,msg is " + ExceptionUtil.getErrorMessage(e));
        }
    }

}
