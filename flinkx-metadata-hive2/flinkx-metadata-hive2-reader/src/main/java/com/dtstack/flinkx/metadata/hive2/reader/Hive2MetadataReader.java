package com.dtstack.flinkx.metadata.hive2.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.metadata.hive2.inputformat.Hive2MetadataInputFormat;
import com.dtstack.flinkx.metadata.reader.inputformat.MetaDataInputFormatBuilder;
import com.dtstack.flinkx.metadata.reader.reader.MetaDataReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class Hive2MetadataReader extends MetaDataReader {
    public Hive2MetadataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        driverName = "org.apache.hive.jdbc.HiveDriver";
    }

    @Override
    protected MetaDataInputFormatBuilder getBuilder(){
        return new MetaDataInputFormatBuilder(new Hive2MetadataInputFormat());
    }
}
