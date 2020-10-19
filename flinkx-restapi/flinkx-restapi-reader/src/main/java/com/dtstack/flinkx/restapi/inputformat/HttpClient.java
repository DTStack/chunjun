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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.dtstack.flinkx.restapi.common.RestContext;
import com.dtstack.flinkx.restapi.common.handler.Handler;
import com.dtstack.flinkx.restapi.common.handler.ReadRecordException;
import com.dtstack.flinkx.restapi.common.handler.ResponseRetryException;
import com.dtstack.flinkx.restapi.common.httprequestApi;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.types.Row;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * httpClient
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/25
 */
public class HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private ScheduledExecutorService scheduledExecutorService;
    protected transient CloseableHttpClient httpClient;
    private final long intervalTime;
    private BlockingQueue<Row> queue;
    private RestContext restContext;
    private AtomicInteger atomicInteger = new AtomicInteger(0);
    private static final String THREAD_NAME = "restApiReader-thread";
    private List<MetaColumn> metaColumns;
    private List<Handler> handlers;


    public HttpClient(RestContext restContext, Long intervalTime, List<MetaColumn> metaColumns) {
        this.restContext = restContext;
        this.intervalTime = intervalTime;
        queue = new SynchronousQueue<>(false);
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, THREAD_NAME);
            }
        });
        this.httpClient = HttpUtil.getHttpClient();
        this.metaColumns = metaColumns;
    }

    public void start() {

        scheduledExecutorService.scheduleAtFixedRate(
                this::execute,
                0,
                intervalTime,
                TimeUnit.MILLISECONDS
        );
    }

    public Row takeEvent() {
        Row row = null;
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;
    }

    public void execute() {
        int i = atomicInteger.incrementAndGet();
        httprequestApi.Httprequest build = restContext.build();
        doExecute(build, 2);
        System.out.println("第" + i + "次请求值" + build);
        restContext.updateValue();
    }

    public void doExecute(httprequestApi.Httprequest build, int retryTime) {

        HttpUriRequest request = HttpUtil.getRequest(restContext.getRequestType(), build.getBody(), build.getHeader(), restContext.getUrl());
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(request);
            HttpEntity entity = httpResponse.getEntity();
            if (entity != null) {
                String entityData = EntityUtils.toString(entity);
                if (restContext.getFormat().equals("json")) {
                    Map<String, Object> map = HttpUtil.gson.fromJson(entityData, Map.class);
                    //todo
                    for (Handler handler : handlers) {
                        if (handler.isPipei(map)) {
                            handler.execute(map);
                        }
                    }
                    if (CollectionUtils.isEmpty(metaColumns) || (metaColumns.size() == 1 && metaColumns.get(0).getName().equals(ConstantValue.STAR_SYMBOL))) {
                        queue.put(Row.of(map));
                    }else{
                        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
                        for (MetaColumn metaColumn : metaColumns) {
                            String[] names = metaColumn.getName().split("\\.");
                            Map<String, Object> keyToMap = initData(stringObjectHashMap, names);
                            Object data = getData(map, names);
                            keyToMap.put(names[names.length - 1], data);
                        }
                        queue.put(Row.of(stringObjectHashMap));
                    }
                } else {
                    queue.put(Row.of(entityData));
                }
            } else {
                throw new RuntimeException("entity is null");
            }
        } catch (ResponseRetryException e) {
            //todo 重试
            if (--retryTime > 0) {
                doExecute(build, retryTime);
            }
        } catch (Exception e) {
            //todo 脏数据处理
            throw new ReadRecordException("get entity error");
        }

    }

    public void close() {
        HttpUtil.closeClient(httpClient);
        scheduledExecutorService.shutdown();
    }

    public Map<String, Object> initData(HashMap<String, Object> data, String[] names) {
        HashMap<String, Object> tempHashMap = data;
        for (int i = 0; i < names.length; i++) {
            if (i != names.length - 1) {
                HashMap<String, Object> objectObjectHashMap = new HashMap<String, Object>(4);
                tempHashMap.putIfAbsent(names[i], objectObjectHashMap);
                tempHashMap = objectObjectHashMap;
            } else {
                tempHashMap.putIfAbsent(names[i], null);
            }
        }
        return tempHashMap;
    }

    public Object getData(Map<String, Object> data, String[] names) {
        //metaColumns有可能为空 或者 有可能为*
        Map<String, Object> tempHashMap = data;
        for (int i = 0; i < names.length; i++) {
            if (tempHashMap.containsKey(names[i]) && i != names.length - 1) {
                if (tempHashMap.get(names[i]) instanceof Map) {
                    tempHashMap = (Map<String, Object>) tempHashMap.get(names[i]);
                } else {
                    return null;
                }
            } else if (i == names.length - 1) {
                return tempHashMap.get(names[i]);
            } else {
                return null;
            }
        }
        return null;
    }
}
