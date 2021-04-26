package com.dtstack.flinkx.metadatakafka.reader;

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.config.DataTransferConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
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

    private String job;

    @Before
    public void setup(){
         job = "{\n" +
                 "  \"job\": {\n" +
                 "    \"content\": [{\n" +
                 "        \"reader\":{\n" +
                 "          \"parameter\": {\n" +
                 "             \"topicList\" : [],\n" +
                 "             \"consumerSettings\" : {\n" +
                 "\t\t       \"bootstrap.servers\": \"127.0.0.1:9092\"\n" +
                 "              }\n" +
                 "            },\n" +
                 "             \"name\": \"metadatakafkareader\"\n" +
                 "\t\t},\n" +
                 "\t\t\"writer\":{\n" +
                 "        \"parameter\" : {\n" +
                 "          \"print\": true\n" +
                 "        },\n" +
                 "        \"name\" : \"streamwriter\"\n" +
                 "      }\n" +
                 "      \n" +
                 "    }],\n" +
                 "    \"setting\": {\n" +
                 "      \"errorLimit\": {\n" +
                 "        \"record\": 100\n" +
                 "      },\n" +
                 "      \"speed\": {\n" +
                 "        \"bytes\": 1048576,\n" +
                 "        \"channel\": 2\n" +
                 "      }\n" +
                 "    }\n" +
                 "  }\n" +
                 "}";



    }

    @Test
    public void metadatakafkareaderTest(){
        Configuration conf = new Configuration();
        conf.setString("akka.ask.timeout", "180 s");
        conf.setString("web.timeout", String.valueOf(100000));
        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);
        PowerMockito.when(new MetadatakafkaReader(DataTransferConfig.parse(job), env)).thenCallRealMethod();
    }

}
