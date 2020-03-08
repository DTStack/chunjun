package com.dtstack.flinkx.hive2metadatasync.reader.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.hive2metadatasync.reader.inputformat.Hive2MetaDataSyncInputFormat;
import com.dtstack.flinkx.metadata.reader.inputformat.MetaDataInputFormatBuilder;
import com.dtstack.flinkx.metadata.reader.reader.MetaDataReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description :
 */
public class Hive2MetaDataSyncReader extends MetaDataReader {

    public Hive2MetaDataSyncReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        driverName = "org.apache.hive.jdbc.HiveDriver";
    }

    @Override
    protected MetaDataInputFormatBuilder getBuilder(){
        return new MetaDataInputFormatBuilder(new Hive2MetaDataSyncInputFormat());
    }
}
