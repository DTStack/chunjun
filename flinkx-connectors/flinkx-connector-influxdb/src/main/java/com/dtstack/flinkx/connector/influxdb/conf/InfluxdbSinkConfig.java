package com.dtstack.flinkx.connector.influxdb.conf;

import com.dtstack.flinkx.sink.WriteMode;

/**
 * @Author xirang
 * @Company Dtstack
 * @Date: 2022/3/14 6:00 PM
 */
public class InfluxdbSinkConfig extends InfluxdbConfig{

    /**
     * retention policy for influxdb writer
     * */
    private String rp;

    /**
     * write mode for influxdb writer
     * */
    private WriteMode writeMode = WriteMode.INSERT;






}
