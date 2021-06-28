package com.dtstack.flinkx.conf;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;

public class MetricParam {

    private RuntimeContext context;
    private boolean makeTaskFailedWhenReportFailed;
    private Map<String,Object> metricPluginConf;

    public MetricParam(
            RuntimeContext context,
            boolean makeTaskFailedWhenReportFailed,
            Map<String,Object> metricPluginConf) {
        this.context = context;
        this.makeTaskFailedWhenReportFailed = makeTaskFailedWhenReportFailed;
        this.metricPluginConf = metricPluginConf;
    }

    public RuntimeContext getContext() {
        return context;
    }

    public void setContext(RuntimeContext context) {
        this.context = context;
    }

    public boolean isMakeTaskFailedWhenReportFailed() {
        return makeTaskFailedWhenReportFailed;
    }

    public void setMakeTaskFailedWhenReportFailed(boolean makeTaskFailedWhenReportFailed) {
        this.makeTaskFailedWhenReportFailed = makeTaskFailedWhenReportFailed;
    }

    public Map<String, Object> getMetricPluginConf() {
        return metricPluginConf;
    }

    public void setMetricPluginConf(Map<String, Object> metricPluginConf) {
        this.metricPluginConf = metricPluginConf;
    }
}

