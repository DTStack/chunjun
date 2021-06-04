package com.dtstack.flinkx.metadatakafka.entity;

import java.io.Serializable;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/21 16:15
 */
public class KafkaConsumerInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     *   消费者组
     */
    private String groupId;

    /**
     * 当前分区
     */
    private Integer partition;

    /**
     * 当前消费 offset
     */
    private Long currentOffset;

    /**
     * 属于 topic
     */
    private String topic;

    /**
     *  broker host
     */
    private String brokerHost;

    /**
     * 未消费数据
     */
    private Long lag;

    /**
     * 当前分区 leader 最后一次提交的offset 也就是当前分区的最大偏移量
     */
    private Long logEndOffset;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(Long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public void setBrokerHost(String brokerHost) {
        this.brokerHost = brokerHost;
    }

    public Long getLag() {
        return lag;
    }

    public void setLag(Long lag) {
        this.lag = lag;
    }

    public Long getLogEndOffset() {
        return logEndOffset;
    }

    public void setLogEndOffset(Long logEndOffset) {
        this.logEndOffset = logEndOffset;
    }

    @Override
    public String toString() {
        return "KafkaConsumerInfo{" +
                "groupId='" + groupId + '\'' +
                ", partition=" + partition +
                ", currentOffset=" + currentOffset +
                ", topic='" + topic + '\'' +
                ", brokerHost='" + brokerHost + '\'' +
                ", lag=" + lag +
                ", logEndOffset=" + logEndOffset +
                '}';
    }
}
