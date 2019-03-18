package com.dtstack.flinkx.metrics;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/3/18
 */
public class InputMetric {

    protected transient Counter numRecordsIn;
    protected transient Meter numRecordsInRate;

    protected transient Counter numBytesIn;
    protected transient Meter numInBytesRate;

    private transient RuntimeContext runtimeContext;

    public InputMetric(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;

        initMetric();
    }

    public void initMetric() {

        numRecordsIn = getRuntimeContext().getMetricGroup().counter(DTMetricNames.IO_NUM_RECORDS_IN);
        numRecordsInRate = getRuntimeContext().getMetricGroup().meter(DTMetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn, 20));

        numBytesIn = getRuntimeContext().getMetricGroup().counter(DTMetricNames.IO_NUM_BYTES_IN);
        numInBytesRate = getRuntimeContext().getMetricGroup().meter(DTMetricNames.IO_NUM_BYTES_IN_RATE, new MeterView(numBytesIn, 20));
    }

    public Counter getNumRecordsIn() {
        return numRecordsIn;
    }

    public Meter getNumRecordsInRate() {
        return numRecordsInRate;
    }

    public Counter getNumBytesIn() {
        return numBytesIn;
    }

    public Meter getNumInBytesRate() {
        return numInBytesRate;
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}
