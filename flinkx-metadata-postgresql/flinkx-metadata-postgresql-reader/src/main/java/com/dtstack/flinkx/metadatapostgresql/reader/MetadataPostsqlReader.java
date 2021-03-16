package com.dtstack.flinkx.metadatapostgresql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.metadatapostgresql.constants.PostgresqlCons;
import com.dtstack.flinkx.metadatapostgresql.inputformat.MetadataPostgresqlInputFormat;
import com.dtstack.metadata.rdb.builder.MetadatardbBuilder;
import com.dtstack.metadata.rdb.reader.MetadatardbReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shitou
 * @date 2020/12/9 16:21
 */
public class MetadataPostsqlReader extends MetadatardbReader {


    public MetadataPostsqlReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
    }

    @Override
    public MetadatardbBuilder createBuilder() {
        return new MetadatardbBuilder(new MetadataPostgresqlInputFormat());
    }

    @Override
    public String getDriverName() {
        return PostgresqlCons.DRIVER_NAME;
    }

}
