package com.dtstack.flinkx.metrics;

import com.dtstack.flinkx.conf.MetricParam;
import com.dtstack.flinkx.conf.MetricPluginConf;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.CharacterFilter;

import java.util.regex.Pattern;

public abstract class CustomReporter {

    protected boolean makeTaskFailedWhenReportFailed;

    protected RuntimeContext context;

    protected static final Pattern UN_ALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    protected static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
        @Override
        public String filterCharacters(String input) {
            return replaceInvalidChars(input);
        }
    };
    protected final CharacterFilter labelValueCharactersFilter = CHARACTER_FILTER;
    public CustomReporter(MetricParam metricParam) {
        this.context = metricParam.getContext();
        this.makeTaskFailedWhenReportFailed = metricParam.isMakeTaskFailedWhenReportFailed();
    }

    static String replaceInvalidChars(final String input) {
        // https://prometheus.io/docs/instrumenting/writing_exporters/
        // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to an underscore.
        return UN_ALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }


    public abstract void open();

    public abstract void registerMetric(Accumulator accumulator, String name);

    public abstract void report();

    public abstract void close();
}
