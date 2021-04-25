package com.dtstack.flinkx.metadatakafka.inputformat;

import org.apache.flink.core.io.InputSplit;

import java.util.List;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/21 15:03
 */
public class MetadatakafkaInputSplit implements InputSplit {

    private Integer SplitNumber;

    private List<String> topicList;


    public MetadatakafkaInputSplit(Integer splitNumber, List<String> topicList) {
        SplitNumber = splitNumber;
        this.topicList = topicList;
    }

    public void setSplitNumber(Integer splitNumber) {
        SplitNumber = splitNumber;
    }

    public List<String> getTopicList() {
        return topicList;
    }

    public void setTopicList(List<String> topicList) {
        this.topicList = topicList;
    }

    @Override
    public int getSplitNumber() {
        return SplitNumber;
    }

    @Override
    public String toString() {
        return "MetadatakafkaInputSplit{" +
                "SplitNumber=" + SplitNumber +
                ", topicList=" + topicList +
                '}';
    }
}
