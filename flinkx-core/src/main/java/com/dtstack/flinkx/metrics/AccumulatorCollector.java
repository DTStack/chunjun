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


package com.dtstack.flinkx.metrics;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.log.DtLogger;
import com.dtstack.flinkx.util.UrlUtil;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

/**
 * Regularly get statistics from the flink API
 *
 * @author jiangbo
 * @date 2019/7/17
 */
public class AccumulatorCollector {

    private static final Logger LOG = LoggerFactory.getLogger(AccumulatorCollector.class);

    private static final String THREAD_NAME = "accumulator-collector-thread";

    private static final String KEY_ACCUMULATORS = "user-task-accumulators";
    private static final String KEY_NAME = "name";
    private static final String KEY_VALUE = "value";

    private static final int MAX_COLLECT_ERROR_TIMES = 100;

    private Gson gson = new Gson();

    private RuntimeContext context;

    private String jobId;

    private List<String> monitorUrls = Lists.newArrayList();

    private int period = 2;

    private CloseableHttpClient httpClient;

    private boolean isLocalMode;

    private ScheduledExecutorService scheduledExecutorService;

    private Map<String, ValueAccumulator> valueAccumulatorMap;

    private List<String> metricNames;

    private long collectErrorTimes = 0;

    public AccumulatorCollector(String jobId, String monitorUrlStr, RuntimeContext runtimeContext, int period, List<String> metricNames){
        Preconditions.checkArgument(jobId != null && jobId.length() > 0);
        Preconditions.checkArgument(period > 0);
        Preconditions.checkArgument(metricNames != null && metricNames.size() > 0);

        this.context = runtimeContext;
        this.period = period;
        this.jobId = jobId;
        this.metricNames = metricNames;

        isLocalMode = StringUtils.isEmpty(monitorUrlStr);

        initValueAccumulatorMap();

        if(!isLocalMode){
            formatMonitorUrl(monitorUrlStr);
            checkMonitorUrlIsValid();

            httpClient = HttpClientBuilder.create().build();
        }

        initThreadPool();
    }

    private void initValueAccumulatorMap(){
        valueAccumulatorMap = new HashMap<>(metricNames.size());
        for (String metricName : metricNames) {
            valueAccumulatorMap.put(metricName, new ValueAccumulator(0, context.getLongCounter(metricName)));
        }
    }

    private void formatMonitorUrl(String monitorUrlStr){
        if(monitorUrlStr.startsWith(ConstantValue.KEY_HTTP)){
            String url;
            if(monitorUrlStr.endsWith(ConstantValue.SINGLE_SLASH_SYMBOL)){
                url = monitorUrlStr + "jobs/" + jobId + "/accumulators";
            } else {
                url = monitorUrlStr + "/jobs/" + jobId + "/accumulators";
            }
            monitorUrls.add(url);
        } else {
            String[] monitor = monitorUrlStr.split(",");
            for (int i = 0; i < monitor.length; ++i) {
                String url = "http://" + monitor[i] + "/jobs/" + jobId + "/accumulators";
                monitorUrls.add(url);
            }
        }
        if(DtLogger.isEnableDebug()){
            LOG.debug("monitorUrls = {}", gson.toJson(monitorUrls));
        }
    }

    private void checkMonitorUrlIsValid(){
        for (String monitorUrl : monitorUrls) {
            try(InputStream ignored = UrlUtil.open(monitorUrl)) {
                return;
            } catch (Exception e) {
                LOG.warn("Connect error with monitor url:{}", monitorUrl);
            }
        }

        isLocalMode = true;
        LOG.info("No valid url，will use local mode");
    }

    private void initThreadPool(){
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,THREAD_NAME);
            }
        });
    }

    public void start(){
        scheduledExecutorService.scheduleAtFixedRate(
                this::collectAccumulator,
                0,
                (long) (period * 1000),
                TimeUnit.MILLISECONDS
        );
    }

    public void close(){
        if(scheduledExecutorService != null && !scheduledExecutorService.isShutdown() && !scheduledExecutorService.isTerminated()) {
            scheduledExecutorService.shutdown();
        }

        if(isLocalMode){
            return;
        }

        try {
            if(httpClient != null){
                httpClient.close();
            }
        } catch (Exception e){
            LOG.error("Close httpClient error:", e);
        }
    }

    public void collectAccumulator(){
        if(!isLocalMode){
            collectAccumulatorWithApi();
        }
    }

    public long getAccumulatorValue(String name){
        ValueAccumulator valueAccumulator = valueAccumulatorMap.get(name);
        if(valueAccumulator == null){
            return 0;
        }

        if(isLocalMode){
            return valueAccumulator.getLocal().getLocalValue();
        } else {
            return valueAccumulator.getGlobal();
        }
    }

    public long getLocalAccumulatorValue(String name){
        ValueAccumulator valueAccumulator = valueAccumulatorMap.get(name);
        if(valueAccumulator == null){
            return 0;
        }

        return valueAccumulator.getLocal().getLocalValue();
    }

    @SuppressWarnings("unchecked")
    private void collectAccumulatorWithApi(){
        for (String monitorUrl : monitorUrls) {
            try {
                String response = UrlUtil.get(httpClient, monitorUrl);
                Map<String,Object> map = gson.fromJson(response, Map.class);
                List<LinkedTreeMap> userTaskAccumulators = (List<LinkedTreeMap>) map.get(KEY_ACCUMULATORS);
                for(LinkedTreeMap accumulator : userTaskAccumulators) {
                    String name = (String) accumulator.get(KEY_NAME);
                    if(name != null && !"tableCol".equalsIgnoreCase(name)) {
                        String accValue = (String) accumulator.get(KEY_VALUE);
                        if(!"null".equals(accValue)){
                            long value = Double.valueOf(accValue).longValue();
                            ValueAccumulator valueAccumulator = valueAccumulatorMap.get(name);
                            if(valueAccumulator != null){
                                valueAccumulator.setGlobal(value);
                            }
                        }
                    }
                }
            } catch (Exception e){
                checkErrorTimes();
                LOG.error("Update data error,url:[{}],error info:", monitorUrl, e);
            }
            break;
        }
    }

    /**
     * 限制最大出错次数，超过最大次数则使任务失败，如果不失败，统计数据没有及时更新，会影响速率限制，错误控制等功能
     */
    private void checkErrorTimes() {
        collectErrorTimes++;
        if (collectErrorTimes > MAX_COLLECT_ERROR_TIMES){
            // 主动关闭线程和资源，防止异常情况下没有关闭
            close();
            throw new RuntimeException("更新统计数据出错次数超过最大限制100次，为了确保数据正确性，任务自动失败");
        }
    }

    class ValueAccumulator{
        private long global;
        private LongCounter local;

        public ValueAccumulator(long global, LongCounter local) {
            this.global = global;
            this.local = local;
        }

        public long getGlobal() {
            return global;
        }

        public LongCounter getLocal() {
            return local;
        }

        public void setGlobal(long global) {
            this.global = global;
        }
    }
}
