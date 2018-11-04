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

package com.dtstack.flinkx.writer;

import com.dtstack.flinkx.util.URLUtil;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Error Limitation
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ErrorLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorLimiter.class);
    private double samplePeriod = 2;
    private ScheduledExecutorService scheduledExecutorService;
    private final Integer maxErrors;
    private final Double maxErrorRatio;
    private String[] monitorUrls;
    private volatile int errors = 0;
    private volatile double errorRatio = 0.0;
    private volatile int numRead = 0;
    private boolean valid = false;
    private String jobId;
    private String taskId;
    private String errMsg = "";

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public ErrorLimiter(RuntimeContext runtimeContext, String monitors, int maxErrors, double samplePeriod) {
        this(runtimeContext, monitors, maxErrors, Double.MAX_VALUE, 1);
    }

    public ErrorLimiter(RuntimeContext runtimeContext, String monitors, Integer maxErrors, Double maxErrorRatio, double samplePeriod) {

        Preconditions.checkArgument(runtimeContext != null || monitors != null, "Should specify rumtimeContext or monitorUrls");
        Preconditions.checkArgument(samplePeriod > 0);
        StreamingRuntimeContext context = (StreamingRuntimeContext) runtimeContext;
        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        this.jobId = vars.get("<job_id>");
        this.taskId = vars.get("<task_id>");

        this.maxErrors = maxErrors;
        this.samplePeriod = samplePeriod;
        this.maxErrorRatio = maxErrorRatio;

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
            try (InputStream inputStream = URLUtil.open(url)){
                 break;
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("connected error: " + url);
            }
        }

        if(j <  monitorUrls.length) {
            valid = true;
        } else {
            return;
        }

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public boolean isValid() {
        return valid;
    }

    public void start() {

        if(scheduledExecutorService == null) {
            return;
        }

        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    Gson gson = new Gson();
                    for(int index = 0; index < monitorUrls.length; ++index) {
                        String requestUrl = monitorUrls[index] + "/jobs/" + jobId + "/accumulators";
                        try(InputStream inputStream = URLUtil.open(requestUrl) ) {
                            try(Reader rd = new InputStreamReader(inputStream)) {
                                Map<String,Object> map = gson.fromJson(rd, Map.class);
                                List<LinkedTreeMap> userTaskAccumulators = (List<LinkedTreeMap>) map.get("user-task-accumulators");
                                for(LinkedTreeMap accumulator : userTaskAccumulators) {
                                    String name = (String) accumulator.get("name");
                                    if(name != null) {
                                        if(name.equals("nErrors")) {
                                            this.errors = Double.valueOf((String) accumulator.get("value")).intValue();
                                        } else if(name.equals("numRead")) {
                                            this.numRead = Double.valueOf((String) accumulator.get("value")).intValue();
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                },
                0,
                (long) (samplePeriod * 1000),
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        if(scheduledExecutorService != null && !scheduledExecutorService.isShutdown() && !scheduledExecutorService.isTerminated()) {
            scheduledExecutorService.shutdown();
        }
    }

    public void acquire() {
        if(isValid()) {
            if(maxErrors != null){
                Preconditions.checkArgument(errors <= maxErrors, "WritingRecordError: error writing record [" + errors + "] exceed limit [" + maxErrors
                        + "]\n" + errMsg);
            }

            if(maxErrorRatio != null){
                if(numRead >= 1) {
                    errorRatio = (double)errors / numRead;
                }
                Preconditions.checkArgument(errorRatio <= maxErrorRatio, "WritingRecordError: error writing record ratio [" + errorRatio + "] exceed limit [" + maxErrorRatio
                        + "]\n" + errMsg);
            }
        }
    }

}
