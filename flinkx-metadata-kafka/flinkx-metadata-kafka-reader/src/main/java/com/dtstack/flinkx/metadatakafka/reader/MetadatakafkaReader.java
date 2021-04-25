package com.dtstack.flinkx.metadatakafka.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.metadatakafka.inputformat.MetadatakafkaInputFormat;
import com.dtstack.flinkx.metadatakafka.inputformat.MetadatakafkaInputFormatBuilder;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/21 14:08
 */
public class MetadatakafkaReader extends BaseDataReader {

    private static final String KEY_CONSUMER_SETTINGS = "consumerSettings";

    private static final String KEY_KERBEROS_CONFIG = "kerberosConfig";

    private static final String KEY_TOPIC_LIST = "topicList";

    private List<String> topicList;

    private Map<String, String> consumerSettings;

    private Map<String, Object> kerberosConfig;

    @SuppressWarnings("unchecked")
    public MetadatakafkaReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        consumerSettings = (Map<String, String>) readerConfig.getParameter().getVal(KEY_CONSUMER_SETTINGS);
        kerberosConfig = (Map<String, Object>) readerConfig.getParameter().getVal(KEY_KERBEROS_CONFIG);
        topicList = (List<String>) readerConfig.getParameter().getVal(KEY_TOPIC_LIST);
    }

    @Override
    public DataStream<Row> readData() {
        MetadatakafkaInputFormatBuilder builder = getBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setConsumerSettings(consumerSettings);
        builder.setKerberosConfig(kerberosConfig);
        builder.setTopicList(topicList);
        return createInput(builder.finish());
    }

    private MetadatakafkaInputFormatBuilder getBuilder(){
        return new MetadatakafkaInputFormatBuilder(new MetadatakafkaInputFormat());
    }
}
