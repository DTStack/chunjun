package com.dtstack.flinkx.connector.influxdb.enums;

import java.util.concurrent.TimeUnit;

/** @Author xirang @Company Dtstack @Date: 2022/3/16 3:32 PM */
public enum TimePrecisionEnums {
    NS("NS", TimeUnit.NANOSECONDS),
    U("U", TimeUnit.MICROSECONDS),
    MS("MS", TimeUnit.MILLISECONDS),
    S("S", TimeUnit.SECONDS),
    M("M", TimeUnit.MINUTES),
    H("H", TimeUnit.HOURS);

    private String desc;

    private TimeUnit precision;

    public TimeUnit getPrecision() {
        return this.precision;
    }

    public static TimePrecisionEnums of(String desc) {
        for (TimePrecisionEnums precision : TimePrecisionEnums.values()) {
            if (precision.desc.equalsIgnoreCase(desc)) return precision;
        }
        return TimePrecisionEnums.NS;
    }

    TimePrecisionEnums(String desc, TimeUnit precision) {
        this.desc = desc;
        this.precision = precision;
    }
}
