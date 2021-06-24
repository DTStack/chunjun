package com.dtstack.flinkx.conf;

import org.apache.flink.api.common.functions.RuntimeContext;

public class MetricParam {

    private RuntimeContext context;
    private boolean makeTaskFailedWhenReportFailed;
    private MetricPluginConf metricPluginConf;

    public MetricParam(
            RuntimeContext context,
            boolean makeTaskFailedWhenReportFailed,
            MetricPluginConf metricPluginConf) {
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

    public MetricPluginConf getMetricPluginConf() {
        return metricPluginConf;
    }

    public void setMetricPluginConf(MetricPluginConf metricPluginConf) {
        this.metricPluginConf = metricPluginConf;
    }
}

