package com.dtstack.flinkx.metadatakafka.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/21 17:16
 */
public class MetadatakafkaEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * topic name
     */
    private String topicName;
    /**
     * 分区数
     */
    private Integer partitions;
    /**
     * 副本数
     */
    private Integer replicationFactor;

    /**
     * 时间戳
     */
    private String timeStamp;

    /**
     * 消费组信息
     */
    private List<GroupInfo> groupInfo;

    /**
     * 是否查询成功
     */
    private boolean querySuccess;

    /**
     * 错误信息
     */
    private String  errorMsg;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getPartitions() {
        return partitions;
    }

    public void setPartitions(Integer partitions) {
        this.partitions = partitions;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public List<GroupInfo> getGroupInfo() {
        return groupInfo;
    }

    public void setGroupInfo(List<GroupInfo> groupInfo) {
        this.groupInfo = groupInfo;
    }

    public boolean isQuerySuccess() {
        return querySuccess;
    }

    public void setQuerySuccess(boolean querySuccess) {
        this.querySuccess = querySuccess;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @Override
    public String toString() {
        return "MetadatakafkaEntity{" +
                "topicName='" + topicName + '\'' +
                ", partitions=" + partitions +
                ", replicationFactor=" + replicationFactor +
                ", timeStamp='" + timeStamp + '\'' +
                ", groupInfo=" + groupInfo +
                ", querySuccess=" + querySuccess +
                ", errorMsg='" + errorMsg + '\'' +
                '}';
    }
}
