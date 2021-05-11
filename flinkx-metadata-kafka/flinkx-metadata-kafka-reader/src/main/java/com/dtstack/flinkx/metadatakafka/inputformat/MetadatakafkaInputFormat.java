package com.dtstack.flinkx.metadatakafka.inputformat;


import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.metadatakafka.entity.GroupInfo;
import com.dtstack.flinkx.metadatakafka.entity.KafkaConsumerInfo;
import com.dtstack.flinkx.metadatakafka.entity.MetadatakafkaEntity;
import com.dtstack.flinkx.metadatakafka.utils.KafkaUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/21 14:10
 */
public class MetadatakafkaInputFormat extends BaseRichInputFormat {

    private static final String KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";

    private static final String KEY_PARTITIONS = "partitions";

    private static final String KEY_REPLICAS = "replicas";

    /**
     * 由于一次任务只针对某一个bootstrap.servers，所以分片数为1
     */
    private static final Integer SPLIT_SIZE = 1;

    /**
     * topic list
     */
    private List<String> topicList;

    /**
     * topic集合迭代器
     */
    private Iterator<String> iterator;

    /**
     * kafka consumer配置参数
     */
    private Map<String, String> consumerSettings;

    /**
     * kafka properties
     */
    private Properties properties;


    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit : {} ", inputSplit);
        topicList = ((MetadatakafkaInputSplit)inputSplit).getTopicList();
        properties = KafkaUtil.initProperties(consumerSettings);
        doOpenInternal();
        iterator = topicList.iterator();
    }

    public void doOpenInternal() {
        if (CollectionUtils.isEmpty(topicList)){
            try {
                topicList = KafkaUtil.getTopicListFromBroker(properties);
            } catch (Exception e) {
                LOG.error("failed to query topic list,bootstrap.servers = {} ",consumerSettings.get(KEY_BOOTSTRAP_SERVERS));
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNumber) throws Exception {
        InputSplit[] inputSplits = new MetadatakafkaInputSplit[SPLIT_SIZE];
        inputSplits[0] = new MetadatakafkaInputSplit(splitNumber,topicList);
        return  inputSplits;
    }


    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        String currentTopic = iterator.next();
        MetadatakafkaEntity metadatakafkaEntity = new MetadatakafkaEntity();
        try {
            metadatakafkaEntity = queryMetadata(currentTopic);
            metadatakafkaEntity.setQuerySuccess(true);
        } catch (Exception e) {
            metadatakafkaEntity.setQuerySuccess(false);
            metadatakafkaEntity.setErrorMsg("Caused by: " + e.getCause().getClass() + ":" + e.getCause().getMessage());
        }
        return Row.of(metadatakafkaEntity);
    }

    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !iterator.hasNext();
    }

    /**
     * 执行查询操作
     * @param topic topic
     * @return kafak元数据实体类
     */
    public MetadatakafkaEntity queryMetadata(String topic) throws Exception {
        MetadatakafkaEntity entity = new MetadatakafkaEntity();
        entity.setTopicName(topic);
        Map<String, Integer> countAndReplicas = KafkaUtil.getTopicPartitionCountAndReplicas(properties, topic);
        entity.setPartitions(countAndReplicas.get(KEY_PARTITIONS));
        entity.setReplicationFactor(countAndReplicas.get(KEY_REPLICAS));

        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss");
        entity.setTimeStamp(sdf.format(new Date()));

        List<String> groups = KafkaUtil.listConsumerGroup(properties, topic);
        List<GroupInfo> groupInfos = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(groups)){
            for (String group: groups){
                GroupInfo groupInfo = new GroupInfo();
                List<KafkaConsumerInfo> infos = KafkaUtil.getGroupInfoByGroupId(properties, group, topic);
                groupInfo.setGroupId(group);
                groupInfo.setTopic(topic);
                groupInfo.setPartitionInfo(infos);
                groupInfos.add(groupInfo);
            }
        }else{
            GroupInfo groupInfo = new GroupInfo();
            List<KafkaConsumerInfo> infos = KafkaUtil.getGroupInfoByGroupId(properties, "", topic);
            groupInfo.setTopic(topic);
            groupInfo.setPartitionInfo(infos);
            groupInfos.add(groupInfo);
        }
        entity.setGroupInfo(groupInfos);
        return entity;
    }

    public void setTopicList(List<String> topicList) {
        this.topicList = topicList;
    }

    public void setConsumerSettings(Map<String, String> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }

    public Map<String, String> getConsumerSettings() {
        return consumerSettings;
    }

}
