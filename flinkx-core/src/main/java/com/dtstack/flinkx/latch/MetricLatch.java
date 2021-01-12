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

package com.dtstack.flinkx.latch;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.UrlUtil;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Distributed implementation of Latch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class MetricLatch extends BaseLatch {

    public static Logger LOG = LoggerFactory.getLogger(MetricLatch.class);

    private String metricName;
    private String[] monitorRoots;
    private String jobId;
    private Gson gson = new Gson();
    private RuntimeContext context;
    private static final String METRIC_PREFIX = "latch-";

    private void checkMonitorRoots() {
        boolean flag = false;
        int j = 0;
        StringBuilder exceptionMsg = new StringBuilder();
        for(; j < monitorRoots.length; ++j) {
            String requestUrl = monitorRoots[j] + "/jobs/" + jobId + "/accumulators";
            LOG.info("Monitor url:" + requestUrl);
            try(InputStream inputStream = UrlUtil.open(requestUrl, 10)) {
                flag = true;
                break;
            } catch (Exception e) {
                exceptionMsg.append("Monitor url:").append(requestUrl).append("\n");
                exceptionMsg.append("Error info:\n").append(e.getMessage()).append("\n");
                LOG.error("Open monitor url error:", e);
            }
        }

        if (!flag){
            throw new IllegalArgumentException(exceptionMsg.toString());
        }
    }

    private int getIntMetricVal(String requestUrl) {
        try(InputStream inputStream = UrlUtil.open(requestUrl)) {
            try(Reader rd = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                Map<String,Object> map = gson.fromJson(rd, Map.class);
                LOG.info("requestUrl = {}, and return map = {}", requestUrl, GsonUtil.GSON.toJson(map));
                List<LinkedTreeMap> userTaskAccumulators = (List<LinkedTreeMap>) map.get("user-task-accumulators");
                for(LinkedTreeMap accumulator : userTaskAccumulators) {
                    if(metricName != null && metricName.equals(accumulator.get("name"))) {
                        return Integer.parseInt((String )accumulator.get("value"));
                    }
                }
            } catch (Exception e) {
                return -1;
            }
        } catch (Exception e) {
            return -1;
        }
        return -1;
    }

    public MetricLatch(RuntimeContext context, String monitors, String metricName) {
        this.metricName = METRIC_PREFIX + metricName;
        this.context = context;
        Map<String, String> vars = context.getMetricGroup().getAllVariables();
        jobId = vars.get("<job_id>");

        if(monitors.startsWith(ConstantValue.KEY_HTTP)) {
            monitorRoots = monitors.split(",");
        } else {
            String[] monitor = monitors.split(",");
            monitorRoots = new String[monitor.length];
            for (int i = 0; i < monitorRoots.length; ++i) {
                monitorRoots[i] = "http://" + monitor[i];
            }
        }

        checkMonitorRoots();
    }


    @Override
    public int getVal() {
        for(int index = 0; index < monitorRoots.length; ++index) {
            String requestUrl = monitorRoots[index] + "/jobs/" + jobId + "/accumulators";
            int metricVal = getIntMetricVal(requestUrl);
            if(metricVal != -1) {
                return metricVal;
            }
        }
        return -1;
    }

    @Override
    public void addOne() {
        context.getIntCounter(metricName).add(1);
    }

}
