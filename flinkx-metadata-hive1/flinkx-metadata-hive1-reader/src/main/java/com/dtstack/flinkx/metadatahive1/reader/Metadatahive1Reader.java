package com.dtstack.flinkx.metadatahive1.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.metadatahive2.reader.Metadatahive2Reader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Metadatahive1Reader extends Metadatahive2Reader {

    public static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    public Metadatahive1Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        driverName = DRIVER_NAME;
    }

}
