package com.dtstack.flinkx.metadatakafka.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/25 17:03
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest(MetadatakafkaReader.class)
public class MetadatakafkareaderTest {


    @Test
    public void metadatakafkareaderTest(){
        DataTransferConfig config = PowerMockito.mock(DataTransferConfig.class);
        StreamExecutionEnvironment env = PowerMockito.mock(StreamExecutionEnvironment.class);
        PowerMockito.suppress(PowerMockito.constructor(MetadatakafkaReader.class));
        MetadatakafkaReader reader = new MetadatakafkaReader(config, env);
    }

}
