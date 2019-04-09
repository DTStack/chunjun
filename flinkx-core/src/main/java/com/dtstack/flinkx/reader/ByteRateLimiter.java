/**
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

package com.dtstack.flinkx.reader;

import com.dtstack.flinkx.util.URLUtil;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is user for speed control
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ByteRateLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(ByteRateLimiter.class);

    private RateLimiter rateLimiter;

    private double expectedBytePerSecond = 100 * 1024 * 1024;

    private RuntimeContext context;

    private double samplePeriod = 1;

    private boolean valid = false;

    private String[] monitorUrls;

    private String jobId;

    private String taskId;

    private int subtaskIndex;

    private CloseableHttpClient httpClient;

    private ScheduledExecutorService scheduledExecutorService;

    public ByteRateLimiter(RuntimeContext runtimeContext, String monitors, double expectedBytePerSecond, double samplePeriod) {
        httpClient = HttpClientBuilder.create().build();

        Preconditions.checkNotNull(runtimeContext);
        //DistributedRuntimeUDFContext context = (DistributedRuntimeUDFContext) runtimeContext;
        StreamingRuntimeContext context = (StreamingRuntimeContext) runtimeContext;

        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        this.jobId = vars.get("<job_id>");
        this.taskId = vars.get("<task_id>");
        String subTaskIndex = vars.get("<subtask_index>");
        this.subtaskIndex = Integer.valueOf(subTaskIndex);

        if(monitors.startsWith("http")) {
            monitorUrls = new String[] {monitors};
        } else {
            String[] monitor = monitors.split(",");
            monitorUrls = new String[monitor.length];
            for (int i = 0; i < monitorUrls.length; ++i) {
                monitorUrls[i] = "http://" + monitor[i];
            }
        }

        int j = 0;
        for(; j < monitorUrls.length; ++j) {
            String url = monitorUrls[j];
            LOG.info("monitor_url=" + url);
            try {
                URLUtil.get(httpClient, url);
                break;
            } catch (Exception e) {
                LOG.error("connected error: " + url);
            }
        }

        LOG.info("find j=" + j);

        if(j <  monitorUrls.length) {
            LOG.info("set valid to true");
            valid = true;
        }
        else {
            LOG.info("state invalid: can't access monitor urls");
            return;
        }

        double initialRate = 1000.0;
        this.rateLimiter = RateLimiter.create(initialRate);
        this.expectedBytePerSecond = expectedBytePerSecond;
        this.context = context;
        this.samplePeriod = samplePeriod;
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public boolean isValid() {
        return valid;
    }

    public void start() {
        if (!isValid()) {
            LOG.info("ByteRateLimiter could not start due to invalid state");
            return;
        }

        final int subTaskIndex = this.subtaskIndex;
        final Gson gson = new Gson();

        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    for (int index = 0; index < 1; ++index) {
                        String requestUrl = monitorUrls[index] + "/jobs/" + this.jobId + "/vertices/" + this.taskId;
                        try {
                            String response = URLUtil.get(httpClient, requestUrl);

                            Map<String, Object> map = gson.fromJson(response, Map.class);
                            double thisWriteBytes = 0;
                            double thisWriteRecords = 0;
                            double totalWriteBytes = 0;
                            double totalWriteRecords = 0;

                            List<LinkedTreeMap> list = (List<LinkedTreeMap>) map.get("subtasks");
                            for (int i = 0; i < list.size(); ++i) {
                                LinkedTreeMap subTask = list.get(i);
                                LinkedTreeMap subTaskMetrics = (LinkedTreeMap) subTask.get("metrics");
                                double subWriteBytes = (double) subTaskMetrics.get("write-bytes");
                                double subWriteRecords = (double) subTaskMetrics.get("write-records");
                                if (i == subTaskIndex) {
                                    thisWriteBytes = subWriteBytes;
                                    thisWriteRecords = subWriteRecords;
                                }
                                totalWriteBytes += subWriteBytes;
                                totalWriteRecords += subWriteRecords;
                            }

                            double thisWriteRatio = (totalWriteRecords == 0 ? 0 : thisWriteRecords / totalWriteRecords);

                            if (totalWriteRecords > 1000 && totalWriteBytes != 0 && thisWriteRatio != 0) {
                                double bpr = totalWriteBytes / totalWriteRecords;
                                double permitsPerSecond = expectedBytePerSecond / bpr * thisWriteRatio;
                                rateLimiter.setRate(permitsPerSecond);
                            }

                            break;
                        } catch (Exception e) {
                            LOG.error("Get metrics error:",e);
                        }
                    }
                },
                0,
                (long) (samplePeriod * 1000),
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        if(!isValid()) {
            return;
        }

        if (httpClient != null){
            try {
                httpClient.close();
            } catch (Exception e){
                LOG.error("close httpClient error:{}", e);
            }
        }

        if(scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    public void acquire() {

        if(isValid()) {
            rateLimiter.acquire();
        }

    }

}
