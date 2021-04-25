package com.dtstack.flinkx.metadatakafka.inputformat;



import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;

import java.util.List;
import java.util.Map;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/23 10:44
 */
public class MetadatakafkaInputFormatBuilder extends BaseRichInputFormatBuilder {


    private MetadatakafkaInputFormat format;

    public MetadatakafkaInputFormatBuilder(MetadatakafkaInputFormat format) {
        super.format = this.format = format;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings){
        this.format.setConsumerSettings(consumerSettings);
    }

    public void setKerberosConfig(Map<String, Object> kerberosConfig){
        this.format.setKerberosConfig(kerberosConfig);
    }

    public void setTopicList(List<String> topics){
        this.format.setTopicList(topics);
    }

    @Override
    protected void checkFormat() {
       if (format.getConsumerSettings() == null || format.getConsumerSettings().size() == 0){
           throw new IllegalArgumentException("consumerSettings can not be empty");
       }
    }
}
