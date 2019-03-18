package com.dtstack.flinkx.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/3/18
 */
public class OutputMetric {

    protected transient Counter numErrors;
    protected transient Counter numNullErrors;
    protected transient Counter numDuplicateErrors;
    protected transient Counter numConversionErrors;
    protected transient Counter numOtherErrors;
    protected transient Counter numWrite;

    protected transient Counter numRecordsOut;
    protected transient Meter numRecordsOutRate;

    protected transient Counter numBytesOut;
    protected transient Meter numBytesOutRate;

    private transient RuntimeContext runtimeContext;

    public OutputMetric(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;

        initMetric();
    }

    public void initMetric() {

        numErrors = getRuntimeContext().getMetricGroup().counter(DTMetricNames.NUM_ERRORS);
        numNullErrors = getRuntimeContext().getMetricGroup().counter(DTMetricNames.NUM_NULL_ERRORS);
        numDuplicateErrors = getRuntimeContext().getMetricGroup().counter(DTMetricNames.NUM_DUPLICATE_ERRORS);
        numConversionErrors = getRuntimeContext().getMetricGroup().counter(DTMetricNames.NUM_CONVERSION_ERRORS);
        numOtherErrors = getRuntimeContext().getMetricGroup().counter(DTMetricNames.NUM_OTHER_ERRORS);
        numWrite = getRuntimeContext().getMetricGroup().counter(DTMetricNames.NUM_WRITES);

        numRecordsOut = getRuntimeContext().getMetricGroup().counter(DTMetricNames.IO_NUM_RECORDS_OUT);
        numRecordsOutRate = getRuntimeContext().getMetricGroup().meter(DTMetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut, 20));

        numBytesOut = getRuntimeContext().getMetricGroup().counter(DTMetricNames.IO_NUM_BYTES_OUT);
        numBytesOutRate = getRuntimeContext().getMetricGroup().meter(DTMetricNames.IO_NUM_BYTES_OUT_RATE, new MeterView(numBytesOut, 20));
    }

    public Counter getNumErrors() {
        return numErrors;
    }

    public Counter getNumNullErrors() {
        return numNullErrors;
    }

    public Counter getNumDuplicateErrors() {
        return numDuplicateErrors;
    }

    public Counter getNumConversionErrors() {
        return numConversionErrors;
    }

    public Counter getNumOtherErrors() {
        return numOtherErrors;
    }

    public Counter getNumWrite() {
        return numWrite;
    }

    public Counter getNumRecordsOut() {
        return numRecordsOut;
    }

    public Meter getNumRecordsOutRate() {
        return numRecordsOutRate;
    }

    public Counter getNumBytesOut() {
        return numBytesOut;
    }

    public Meter getNumBytesOutRate() {
        return numBytesOutRate;
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }


}
