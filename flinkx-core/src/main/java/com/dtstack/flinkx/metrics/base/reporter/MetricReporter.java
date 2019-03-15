package com.dtstack.flinkx.metrics.base.reporter;


import com.dtstack.flinkx.metrics.base.MetricGroup;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;

/**
 * copy from https://github.com/apache/flink
 *
 * Reporters are used to export {@link Metric Metrics} to an external backend.
 *
 * <p>Reporters are instantiated via reflection and must be public, non-abstract, and have a
 * public no-argument constructor.
 */
public interface MetricReporter {

    /**
     * Configures this reporter. Since reporters are instantiated generically and hence parameter-less,
     * this method is the place where the reporters set their basic fields based on configuration values.
     *
     * <p>This method is always called first on a newly instantiated reporter.
     *
     * @param config A properties object that contains all parameters set for this reporter.
     */
    void open(MetricConfig config);

    /**
     * Closes this reporter. Should be used to close channels, streams and release resources.
     */
    void close();

    // ------------------------------------------------------------------------
    //  adding / removing metrics
    // ------------------------------------------------------------------------

    /**
     * Called when a new {@link Metric} was added.
     *
     * @param metric      the metric that was added
     * @param metricName  the name of the metric
     * @param group       the group that contains the metric
     */
    void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group);

    /**
     * Called when a {@link Metric} was should be removed.
     *
     * @param metric      the metric that should be removed
     * @param metricName  the name of the metric
     * @param group       the group that contains the metric
     */
    void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group);
}
