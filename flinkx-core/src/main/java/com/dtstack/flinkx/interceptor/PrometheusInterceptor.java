package com.dtstack.flinkx.interceptor;

import com.dtstack.flinkx.metrics.CustomPrometheusReporter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.IOException;

public class PrometheusInterceptor implements Interceptor {

    private CustomPrometheusReporter customPrometheusReporter;

    private final Boolean ustomPrometheusReporter;
    private final StreamingRuntimeContext runtimeContext;
    private final Boolean makeTaskFailedWhenReportFailed;

    public PrometheusInterceptor(
            Boolean ustomPrometheusReporter,
            StreamingRuntimeContext runtimeContext, Boolean makeTaskFailedWhenReportFailed) {
        this.ustomPrometheusReporter = ustomPrometheusReporter;
        this.runtimeContext = runtimeContext;
        this.makeTaskFailedWhenReportFailed = makeTaskFailedWhenReportFailed;
    }

    @Override
    public void init(Configuration configuration) {
        if (useCustomPrometheusReporter()) {
            customPrometheusReporter = new CustomPrometheusReporter(runtimeContext, makeTaskFailedWhenReportFailed());
            customPrometheusReporter.open();
        }
    }

    private boolean makeTaskFailedWhenReportFailed() {
        return makeTaskFailedWhenReportFailed;
    }

    private boolean useCustomPrometheusReporter() {
        return ustomPrometheusReporter;
    }

    @Override
    public void pre(Context context) {

    }

    @Override
    public void post(Context context) {

    }

    @Override
    public void close() throws IOException {

        if (useCustomPrometheusReporter() && null != customPrometheusReporter) {
            customPrometheusReporter.report();
        }

        if (useCustomPrometheusReporter() && null != customPrometheusReporter) {
            customPrometheusReporter.close();
        }
    }
}
