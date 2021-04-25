package com.dtstack.flinkx.metadatakafka.inputformat;



import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/25 11:50
 */
@RunWith(PowerMockRunner.class)
public class MetadatakafkaInputFormatTest {

    @Test
    public void testQueryMetadata() throws IllegalAccessException {
        MetadatakafkaInputFormat inputFormat = PowerMockito.mock(MetadatakafkaInputFormat.class);
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        MemberModifier.field(MetadatakafkaInputFormat.class, "consumerSettings").set(inputFormat, consumerSettings);
        PowerMockito.doCallRealMethod().when(inputFormat).queryMetadata("kafka10");
        Assert.assertEquals("kafka10", inputFormat.queryMetadata("kafka10").getTopicName());
    }

}
