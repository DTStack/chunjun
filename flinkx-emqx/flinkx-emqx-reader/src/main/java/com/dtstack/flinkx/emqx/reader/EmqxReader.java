package com.dtstack.flinkx.emqx.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.emqx.format.EmqxInputFormatBuilder;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_BROKER;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_CODEC;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_IS_CLEAN_SESSION;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_PASSWORD;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_QOS;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_TOPIC;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_USERNAME;

/**
 * Date: 2020/02/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class EmqxReader extends BaseDataReader {

    private String broker;
    private String topic;
    private String username;
    private String password;
    private String codec;
    private boolean isCleanSession;
    private int qos;

    public EmqxReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        broker = readerConfig.getParameter().getStringVal(KEY_BROKER);
        topic = readerConfig.getParameter().getStringVal(KEY_TOPIC);
        username = readerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(KEY_PASSWORD);
        codec = readerConfig.getParameter().getStringVal(KEY_CODEC, "plain");
        isCleanSession = readerConfig.getParameter().getBooleanVal(KEY_IS_CLEAN_SESSION, true);
        qos = readerConfig.getParameter().getIntVal(KEY_QOS, 2);
    }

    @Override
    public DataStream<Row> readData() {
        EmqxInputFormatBuilder builder = new EmqxInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setBroker(broker);
        builder.setTopic(topic);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setCodec(codec);
        builder.setCleanSession(isCleanSession);
        builder.setQos(qos);
        builder.setRestoreConfig(restoreConfig);
        return createInput(builder.finish());
    }
}
