package com.dtstack.flinkx.metastore.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.metadata.builder.MetadataBaseBuilder;
import com.dtstack.flinkx.metastore.builder.MetaStoreInputFormatBuilder;
import com.dtstack.flinkx.metastore.inputformat.MetaStoreInputFormat;
import com.dtstack.flinkx.metadata.reader.MetaDataBaseReader;
import com.dtstack.flinkx.metatdata.hive2.core.util.Hive2MetaDataCons;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-04 14:30
 * @Description:
 */
public class MetastoreReader extends MetaDataBaseReader {


    public MetastoreReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hadoopConfig = (Map<String, Object>) readerConfig.getParameter().getVal(Hive2MetaDataCons.KEY_HADOOP_CONFIG);
    }


    @Override
    public MetadataBaseBuilder createBuilder() {
        MetaStoreInputFormatBuilder metadataBuilder = new MetaStoreInputFormatBuilder(new MetaStoreInputFormat());
        metadataBuilder.setHadoopConfig(hadoopConfig);
        return metadataBuilder;
    }
}
