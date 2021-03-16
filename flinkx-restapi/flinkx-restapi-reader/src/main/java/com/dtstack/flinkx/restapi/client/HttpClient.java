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
package com.dtstack.flinkx.restapi.client;

import com.dtstack.flinkx.restapi.common.ConstantValue;
import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
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

import java.util.ArrayList;
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

    private boolean first;

    private final RestHandler restHandler;

    /** 内部重试次数(返回的httpStatus 不是 200 就进行重试) **/
    private int requestRetryTime;

    /** 原始请求body */
    private final List<MetaParam> originalBodyList;

    /** 原始请求param */
    private final List<MetaParam> originalParamList;

    /** 原始请求header */
    private final List<MetaParam> originalHeaderList;

    private final List<MetaParam> allMetaParam = new ArrayList<>(32);

    /** 当前请求参数*/
    private HttpRequestParam currentParam;

    /** 上次请求参数 */
    private HttpRequestParam prevParam;


    /** 上一次请求的返回值 */
    private String prevResponse;

    private boolean reachEnd;

    private boolean running;


    public HttpClient(HttpRestConfig httpRestConfig, List<MetaParam> originalBodyList, List<MetaParam> originalParamList, List<MetaParam> originalHeaderList) {
        this.restConfig = httpRestConfig;
        this.originalHeaderList = originalHeaderList;
        this.originalBodyList = originalBodyList;
        this.originalParamList = originalParamList;
        allMetaParam.addAll(originalHeaderList);
        allMetaParam.addAll(originalBodyList);
        allMetaParam.addAll(originalParamList);


        this.queue = new LinkedBlockingQueue<>();
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, THREAD_NAME));
        this.httpClient = HttpUtil.getHttpsClient();
        this.restHandler = new DefaultRestHandler();

        this.prevResponse = "";
        this.first = true;
        this.currentParam = new HttpRequestParam();
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
        running = true;
    }

    public void initPosition(HttpRequestParam requestParam, String response) {
        this.prevParam = requestParam;
        this.prevResponse = response;
        this.first = false;
    }

    public void execute() {

        if (!running) {
            return;
        }

        Thread.currentThread().setUncaughtExceptionHandler((t, e) ->
                LOG.warn("HttpClient run failed, Throwable = {}, HttpClient->{}", ExceptionUtil.getErrorMessage(e), this.toString()));

        //参数构建
        try {
            // 将返回值尝试转为json 如果decode是json 转换失败了 就直接抛出异常 如果decode是text 就不报错正常走下去
            // 因为动态变量有${response.}格式时，在构建请求参数时，需要传递responseValue，和decode是不是json无关
            Map<String, Object> responseValue = null;
            try {
                responseValue = GsonUtil.GSON.fromJson(prevResponse, GsonUtil.gsonMapTypeToken);
            } catch (Exception e) {
                if (restConfig.isJsonDecode()) {
                    throw e;
                }
            }
            currentParam = restHandler.buildRequestParam(originalParamList, originalBodyList, originalHeaderList, prevParam, responseValue, restConfig, first);
        } catch (Exception e) {
            //如果构建参数失败 任务结束,不需要重试 因为这里面没有网络波动等不可控异常
            ResponseValue value = new ResponseValue(-1, null, ExceptionUtil.getErrorMessage(e), null, null);
            processData(value);
            running = false;
            return;
        }

        LOG.debug("currentParam is {}", currentParam);
        doExecute(com.dtstack.flinkx.restapi.common.ConstantValue.REQUEST_RETRY_TIME);
        first = false;
        requestRetryTime = 3;
    }

    public void doExecute(int retryTime) {

        //重试次数到了 就直接任务结束
        if (retryTime < 0) {
            processData(new ResponseValue(-1, null, "the maximum number of retries has been reached，task closed， httpClient value is " + this.toString(), null, null));
            running = false;
            return;
        }

        //执行请求
        String responseValue = null;
        try {

            HttpUriRequest request = HttpUtil.getRequest(restConfig.getRequestMode(), currentParam.getBody(), currentParam.getParam(), currentParam.getHeader(), restConfig.getUrl());
            CloseableHttpResponse httpResponse = httpClient.execute(request);
            if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                LOG.warn("httpStatus is {} and is not 200 ,try retry", httpResponse.getStatusLine().getStatusCode());
                doExecute(--requestRetryTime);
                return;
            }

            responseValue = EntityUtils.toString(httpResponse.getEntity());
        } catch (Throwable e) {
            //只要本次请求中出现了异常 都会进行重试，如果重试次数达到了就真正结束任务
            LOG.warn("httpClient value is {}, error info is {}", this.toString(), ExceptionUtil.getErrorMessage(e));
            doExecute(--requestRetryTime);
            return;
        }

        // 业务处理
        try {
            //下面方法不会捕捉异常并忽视，出现问题 直接结束，因为下面方法出现异常不会是网络抖动等不可控问题
            Strategy strategy = restHandler.chooseStrategy(restConfig.getStrategy(), restConfig.isJsonDecode() ? GsonUtil.GSON.fromJson(responseValue, GsonUtil.gsonMapTypeToken) : null, restConfig, HttpRequestParam.copy(currentParam),allMetaParam);

            if (strategy != null) {
                //进行策略的执行
                switch (strategy.getHandle()) {
                    case ConstantValue.STRATEGY_RETRY:
                        doExecute(--retryTime);
                        return;
                    case ConstantValue.STRATEGY_STOP:
                        reachEnd = true;
                        running = false;
                        break;
                    default:
                        break;
                }
            }

            ResponseValue value = restHandler.buildResponseValue(restConfig.getDecode(), responseValue, restConfig.getFields(), HttpRequestParam.copy(currentParam));
            if (reachEnd) {
                //如果结束了  需要告诉format 结束了
                if (value.isNormal()) {
                    value.setStatus(0);
                    //触发的策略信息返回上游
                    value.setErrorMsg(strategy.toString());
                }
            }
            processData(value);

            prevParam = currentParam;
            prevResponse = responseValue;
        } catch (Throwable e) {
            //只要出现了异常 就任务结束了
            LOG.warn("httpClient value is {},responseValue is {}, error info is {}", this.toString(), responseValue, ExceptionUtil.getErrorMessage(e));
            processData(new ResponseValue(-1, null, "prevResponse value is " + prevResponse + " exception " + ExceptionUtil.getErrorMessage(e), null, null));
            running = false;
        }
    }

    public void processData(ResponseValue value) {
        try {
            queue.put(Row.of(value));
        } catch (InterruptedException e1) {
            LOG.warn("put value error,value is {},currentParam is {} ,errorInfo is {}", value, currentParam, ExceptionUtil.getErrorMessage(e1));
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

    @Override
    public String toString() {
        return "HttpClient{" +
                ", restConfig=" + restConfig +
                ", first=" + first +
                ", requestRetryTime=" + requestRetryTime +
                ", originalBodyList=" + originalBodyList +
                ", originalParamList=" + originalParamList +
                ", originalHeaderList=" + originalHeaderList +
                ", currentParam=" + currentParam +
                ", prevParam=" + prevParam +
                ", prevResponse='" + prevResponse + '\'' +
                ", reachEnd=" + reachEnd +
                ", running=" + running +
                '}';
    }
}
