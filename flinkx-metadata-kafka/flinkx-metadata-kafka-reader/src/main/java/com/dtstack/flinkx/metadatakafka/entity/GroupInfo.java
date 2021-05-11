package com.dtstack.flinkx.metadatakafka.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/23 9:54
 */
public class GroupInfo  implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     *   消费者组
     */
    private String groupId;

    /**
     * 属于 topic
     */
    private String topic;

    /**
     * 分区消费信息
     */
    private List<KafkaConsumerInfo> partitionInfo;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<KafkaConsumerInfo> getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(List<KafkaConsumerInfo> partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    @Override
    public String toString() {
        return "GroupInfo{" +
                "groupId='" + groupId + '\'' +
                ", topic='" + topic + '\'' +
                ", partitionInfo=" + partitionInfo +
                '}';
    }
}
