/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.test.core;

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.carbondata.writer.CarbondataWriter;
import com.dtstack.flinkx.clickhouse.writer.ClickhouseWriter;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.db2.writer.Db2Writer;
import com.dtstack.flinkx.es.writer.EsWriter;
import com.dtstack.flinkx.ftp.writer.FtpWriter;
import com.dtstack.flinkx.gbase.writer.GbaseWriter;
import com.dtstack.flinkx.hbase.writer.HbaseWriter;
import com.dtstack.flinkx.hdfs.writer.HdfsWriter;
import com.dtstack.flinkx.hive.writer.HiveWriter;
import com.dtstack.flinkx.kafka.writer.KafkaWriter;
import com.dtstack.flinkx.kafka09.writer.Kafka09Writer;
import com.dtstack.flinkx.kafka10.writer.Kafka10Writer;
import com.dtstack.flinkx.kafka11.writer.Kafka11Writer;
import com.dtstack.flinkx.kudu.writer.KuduWriter;
import com.dtstack.flinkx.mongodb.writer.MongodbWriter;
import com.dtstack.flinkx.mysql.writer.MysqlWriter;
import com.dtstack.flinkx.odps.writer.OdpsWriter;
import com.dtstack.flinkx.oracle.writer.OracleWriter;
import com.dtstack.flinkx.polardb.writer.PolardbWriter;
import com.dtstack.flinkx.postgresql.writer.PostgresqlWriter;
import com.dtstack.flinkx.redis.writer.RedisWriter;
import com.dtstack.flinkx.sqlserver.writer.SqlserverWriter;
import com.dtstack.flinkx.stream.writer.StreamWriter;
import com.dtstack.flinkx.test.PluginNameConstrant;
import com.dtstack.flinkx.test.core.result.BaseResult;
import com.dtstack.flinkx.test.core.result.WriterResult;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * @author jiangbo
 */
public class WriterUnitTestLauncher extends UnitTestLauncher {

    private Row[] records;

    public WriterUnitTestLauncher withRecords(Row[] records) {
        this.records = records;
        return this;
    }

    @Override
    public String pluginType() {
        return "writer";
    }

    @Override
    public BaseResult runJob() throws Exception{
        String job = readJob();

        for (ParameterReplace parameterReplace : parameterReplaces) {
            job = parameterReplace.replaceParameter(job);
        }

        DataTransferConfig config = DataTransferConfig.parse(job);
        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();

        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);

        openCheckpointConf(env);

        env.setParallelism(speedConfig.getChannel());
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<Row> dataStream = env.fromElements(records);

        BaseDataWriter writer = buildDataWriter(config);
        writer.writeData(dataStream);

        if(StringUtils.isNotEmpty(savepointPath)){
            env.setSettings(SavepointRestoreSettings.forPath(savepointPath));
        }

        JobExecutionResult executionResult = env.execute();
        return new WriterResult(executionResult);
    }

    private static BaseDataWriter buildDataWriter(DataTransferConfig config){
        String writerName = config.getJob().getContent().get(0).getWriter().getName();
        BaseDataWriter writer;
        switch (writerName){
            case PluginNameConstrant.STREAM_WRITER : writer = new StreamWriter(config); break;
            case PluginNameConstrant.CARBONDATA_WRITER : writer = new CarbondataWriter(config); break;
            case PluginNameConstrant.MYSQL_WRITER : writer = new MysqlWriter(config); break;
            case PluginNameConstrant.SQLSERVER_WRITER : writer = new SqlserverWriter(config); break;
            case PluginNameConstrant.ORACLE_WRITER : writer = new OracleWriter(config); break;
            case PluginNameConstrant.POSTGRESQL_WRITER : writer = new PostgresqlWriter(config); break;
            case PluginNameConstrant.DB2_WRITER : writer = new Db2Writer(config); break;
            case PluginNameConstrant.GBASE_WRITER : writer = new GbaseWriter(config); break;
            case PluginNameConstrant.ES_WRITER : writer = new EsWriter(config); break;
            case PluginNameConstrant.FTP_WRITER : writer = new FtpWriter(config); break;
            case PluginNameConstrant.HBASE_WRITER : writer = new HbaseWriter(config); break;
            case PluginNameConstrant.HDFS_WRITER : writer = new HdfsWriter(config); break;
            case PluginNameConstrant.MONGODB_WRITER : writer = new MongodbWriter(config); break;
            case PluginNameConstrant.ODPS_WRITER : writer = new OdpsWriter(config); break;
            case PluginNameConstrant.REDIS_WRITER : writer = new RedisWriter(config); break;
            case PluginNameConstrant.HIVE_WRITER : writer = new HiveWriter(config); break;
            case PluginNameConstrant.KAFKA09_WRITER : writer = new Kafka09Writer(config); break;
            case PluginNameConstrant.KAFKA10_WRITER : writer = new Kafka10Writer(config); break;
            case PluginNameConstrant.KAFKA11_WRITER : writer = new Kafka11Writer(config); break;
            case PluginNameConstrant.KUDU_WRITER : writer = new KuduWriter(config); break;
            case PluginNameConstrant.CLICKHOUSE_WRITER : writer = new ClickhouseWriter(config); break;
            case PluginNameConstrant.POLARDB_WRITER : writer = new PolardbWriter(config); break;
            case PluginNameConstrant.KAFKA_WRITER : writer = new KafkaWriter(config); break;
            default:throw new IllegalArgumentException("Can not find writer by name:" + writerName);
        }

        return writer;
    }
}
