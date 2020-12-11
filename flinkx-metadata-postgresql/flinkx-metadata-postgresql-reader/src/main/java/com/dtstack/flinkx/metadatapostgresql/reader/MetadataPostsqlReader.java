package com.dtstack.flinkx.metadatapostgresql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.metadata.inputformat.MetadataInputFormatBuilder;
import com.dtstack.flinkx.metadata.reader.MetadataReader;
import com.dtstack.flinkx.metadatapostgresql.constants.PostgresqlCons;
import com.dtstack.flinkx.metadatapostgresql.inputformat.MetadataPostgresqlInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.reader
 *
 * @author shitou
 * @description //TODO
 * @date 2020/12/9 16:21
 */
public class MetadataPostsqlReader extends MetadataReader{

    public MetadataPostsqlReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        driverName = PostgresqlCons.DRIVER_NAME;
    }

    @Override
    protected MetadataInputFormatBuilder getBuilder(){
        return new MetadataInputFormatBuilder(new MetadataPostgresqlInputFormat());
    }
}
