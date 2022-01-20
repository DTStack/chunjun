package com.dtstack.flinkx.connector.inceptor.conf;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;

import java.util.Map;

public class InceptorConf extends JdbcConf {

    private String partition;

    private String partitionType;

    private Map<String, Object> hadoopConfig;

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public Map<String, Object> getHadoopConfig() {
        return hadoopConfig;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }
}
