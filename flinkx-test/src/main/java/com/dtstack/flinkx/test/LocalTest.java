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
package com.dtstack.flinkx.test;

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.binlog.reader.BinlogReader;
import com.dtstack.flinkx.carbondata.reader.CarbondataReader;
import com.dtstack.flinkx.carbondata.writer.CarbondataWriter;
import com.dtstack.flinkx.clickhouse.reader.ClickhouseReader;
import com.dtstack.flinkx.clickhouse.writer.ClickhouseWriter;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.db2.reader.Db2Reader;
import com.dtstack.flinkx.db2.writer.Db2Writer;
import com.dtstack.flinkx.dm.reader.DmReader;
import com.dtstack.flinkx.dm.writer.DmWriter;
import com.dtstack.flinkx.emqx.reader.EmqxReader;
import com.dtstack.flinkx.emqx.writer.EmqxWriter;
import com.dtstack.flinkx.es.reader.EsReader;
import com.dtstack.flinkx.es.writer.EsWriter;
import com.dtstack.flinkx.ftp.reader.FtpReader;
import com.dtstack.flinkx.ftp.writer.FtpWriter;
import com.dtstack.flinkx.gbase.reader.GbaseReader;
import com.dtstack.flinkx.gbase.writer.GbaseWriter;
import com.dtstack.flinkx.greenplum.reader.GreenplumReader;
import com.dtstack.flinkx.greenplum.writer.GreenplumWriter;
import com.dtstack.flinkx.hbase.reader.HbaseReader;
import com.dtstack.flinkx.hbase.writer.HbaseWriter;
import com.dtstack.flinkx.hdfs.reader.HdfsReader;
import com.dtstack.flinkx.hdfs.writer.HdfsWriter;
import com.dtstack.flinkx.hive.writer.HiveWriter;
import com.dtstack.flinkx.kafka.reader.KafkaReader;
import com.dtstack.flinkx.kafka.writer.KafkaWriter;
import com.dtstack.flinkx.kafka10.reader.Kafka10Reader;
import com.dtstack.flinkx.kafka10.writer.Kafka10Writer;
import com.dtstack.flinkx.kafka11.reader.Kafka11Reader;
import com.dtstack.flinkx.kafka11.writer.Kafka11Writer;
import com.dtstack.flinkx.kingbase.reader.KingbaseReader;
import com.dtstack.flinkx.kingbase.writer.KingbaseWriter;
import com.dtstack.flinkx.kudu.reader.KuduReader;
import com.dtstack.flinkx.kudu.writer.KuduWriter;
import com.dtstack.flinkx.mongodb.reader.MongodbReader;
import com.dtstack.flinkx.mongodb.writer.MongodbWriter;
import com.dtstack.flinkx.mysql.reader.MysqlReader;
import com.dtstack.flinkx.mysql.writer.MysqlWriter;
import com.dtstack.flinkx.mysqld.reader.MysqldReader;
import com.dtstack.flinkx.odps.reader.OdpsReader;
import com.dtstack.flinkx.odps.writer.OdpsWriter;
import com.dtstack.flinkx.oracle.reader.OracleReader;
import com.dtstack.flinkx.oracle.writer.OracleWriter;
import com.dtstack.flinkx.phoenix5.reader.Phoenix5Reader;
import com.dtstack.flinkx.phoenix5.writer.Phoenix5Writer;
import com.dtstack.flinkx.polardb.reader.PolardbReader;
import com.dtstack.flinkx.polardb.writer.PolardbWriter;
import com.dtstack.flinkx.postgresql.reader.PostgresqlReader;
import com.dtstack.flinkx.postgresql.writer.PostgresqlWriter;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.redis.writer.RedisWriter;
import com.dtstack.flinkx.sqlserver.reader.SqlserverReader;
import com.dtstack.flinkx.sqlserver.writer.SqlserverWriter;
import com.dtstack.flinkx.stream.reader.StreamReader;
import com.dtstack.flinkx.stream.writer.StreamWriter;
import com.dtstack.flinkx.util.ResultPrintUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
/**
 * @author jiangbo
 */
public class LocalTest {

    private static final int FAILURE_RATE = 3;
    private static final int FAILURE_INTERVAL = 6;
    private static final int DELAY_INTERVAL = 10;
    public static Logger LOG = LoggerFactory.getLogger(LocalTest.class);
    public static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception{
        Properties confProperties = new Properties();
//        confProperties.put("flink.checkpoint.interval", "10000");
//        confProperties.put("flink.checkpoint.stateBackend", "file:///tmp/flinkx_checkpoint");

//        conf.setString("metrics.reporter.promgateway.class","org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
//        conf.setString("metrics.reporter.promgateway.host","127.0.0.1");
//        conf.setString("metrics.reporter.promgateway.port","9091");
//        conf.setString("metrics.reporter.promgateway.jobName","108job");
//        conf.setString("metrics.reporter.promgateway.randomJobNameSuffix","true");
//        conf.setString("metrics.reporter.promgateway.deleteOnShutdown","true");

        String jobPath = "your json file's absolute path";
        JobExecutionResult result = LocalTest.runJob(new File(jobPath), confProperties, null);
        ResultPrintUtil.printResult(result);
        System.exit(0);
    }

    public static JobExecutionResult runJob(File jobFile, Properties confProperties, String savepointPath) throws Exception{
        String jobContent = readJob(jobFile);
        return runJob(jobContent, confProperties, savepointPath);
    }

    public static JobExecutionResult runJob(String job, Properties confProperties, String savepointPath) throws Exception{
        DataTransferConfig config = DataTransferConfig.parse(job);

        conf.setString("akka.ask.timeout", "180 s");
        conf.setString("web.timeout", String.valueOf(100000));

        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);

        openCheckpointConf(env, confProperties);

        env.setParallelism(config.getJob().getSetting().getSpeed().getChannel());

        if (needRestart(config)) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    10,
                    Time.of(10, TimeUnit.SECONDS)
            ));
        }
        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();
        BaseDataReader reader = buildDataReader(config, env);
        DataStream<Row> dataStream = reader.readData();
        if(speedConfig.getReaderChannel() > 0){
            dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speedConfig.getReaderChannel());
        }

        if (speedConfig.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        BaseDataWriter dataWriter = buildDataWriter(config);
        DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
        if(speedConfig.getWriterChannel() > 0){
            dataStreamSink.setParallelism(speedConfig.getWriterChannel());
        }

        if(StringUtils.isNotEmpty(savepointPath)){
            env.setSettings(SavepointRestoreSettings.forPath(savepointPath));
        }

        return env.execute();
    }

    private static boolean needRestart(DataTransferConfig config){
        return config.getJob().getSetting().getRestoreConfig().isRestore();
    }

    private static String readJob(File file) {
        try(FileInputStream in = new FileInputStream(file)) {
            byte[] fileContent = new byte[(int) file.length()];
            in.read(fileContent);
            return new String(fileContent, StandardCharsets.UTF_8);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private static BaseDataReader buildDataReader(DataTransferConfig config, StreamExecutionEnvironment env){
        String readerName = config.getJob().getContent().get(0).getReader().getName();
        BaseDataReader reader ;
        switch (readerName){
            case PluginNameConstants.STREAM_READER : reader = new StreamReader(config, env); break;
            case PluginNameConstants.CARBONDATA_READER : reader = new CarbondataReader(config, env); break;
            case PluginNameConstants.ORACLE_READER : reader = new OracleReader(config, env); break;
            case PluginNameConstants.POSTGRESQL_READER : reader = new PostgresqlReader(config, env); break;
            case PluginNameConstants.SQLSERVER_READER : reader = new SqlserverReader(config, env); break;
            case PluginNameConstants.MYSQLD_READER : reader = new MysqldReader(config, env); break;
            case PluginNameConstants.MYSQL_READER : reader = new MysqlReader(config, env); break;
            case PluginNameConstants.DB2_READER : reader = new Db2Reader(config, env); break;
            case PluginNameConstants.GBASE_READER : reader = new GbaseReader(config, env); break;
            case PluginNameConstants.ES_READER : reader = new EsReader(config, env); break;
            case PluginNameConstants.FTP_READER : reader = new FtpReader(config, env); break;
            case PluginNameConstants.HBASE_READER : reader = new HbaseReader(config, env); break;
            case PluginNameConstants.HDFS_READER : reader = new HdfsReader(config, env); break;
            case PluginNameConstants.MONGODB_READER : reader = new MongodbReader(config, env); break;
            case PluginNameConstants.ODPS_READER : reader = new OdpsReader(config, env); break;
            case PluginNameConstants.BINLOG_READER : reader = new BinlogReader(config, env); break;
            case PluginNameConstants.KAFKA10_READER : reader = new Kafka10Reader(config, env); break;
            case PluginNameConstants.KAFKA11_READER : reader = new Kafka11Reader(config, env); break;
            case PluginNameConstants.KAFKA_READER : reader = new KafkaReader(config, env); break;
            case PluginNameConstants.KUDU_READER : reader = new KuduReader(config, env); break;
            case PluginNameConstants.CLICKHOUSE_READER : reader = new ClickhouseReader(config, env); break;
            case PluginNameConstants.POLARDB_READER : reader = new PolardbReader(config, env); break;
            case PluginNameConstants.EMQX_READER : reader = new EmqxReader(config, env); break;
            case PluginNameConstants.DM_READER : reader = new DmReader(config, env); break;
            case PluginNameConstants.GREENPLUM_READER : reader = new GreenplumReader(config, env); break;
            case PluginNameConstants.PHOENIX5_READER : reader = new Phoenix5Reader(config, env); break;
            case PluginNameConstants.KINGBASE_READER : reader = new KingbaseReader(config, env); break;
            default:throw new IllegalArgumentException("Can not find reader by name:" + readerName);
        }

        return reader;
    }

    private static BaseDataWriter buildDataWriter(DataTransferConfig config){
        String writerName = config.getJob().getContent().get(0).getWriter().getName();
        BaseDataWriter writer;
        switch (writerName){
            case PluginNameConstants.STREAM_WRITER : writer = new StreamWriter(config); break;
            case PluginNameConstants.CARBONDATA_WRITER : writer = new CarbondataWriter(config); break;
            case PluginNameConstants.MYSQL_WRITER : writer = new MysqlWriter(config); break;
            case PluginNameConstants.SQLSERVER_WRITER : writer = new SqlserverWriter(config); break;
            case PluginNameConstants.ORACLE_WRITER : writer = new OracleWriter(config); break;
            case PluginNameConstants.POSTGRESQL_WRITER : writer = new PostgresqlWriter(config); break;
            case PluginNameConstants.DB2_WRITER : writer = new Db2Writer(config); break;
            case PluginNameConstants.GBASE_WRITER : writer = new GbaseWriter(config); break;
            case PluginNameConstants.ES_WRITER : writer = new EsWriter(config); break;
            case PluginNameConstants.FTP_WRITER : writer = new FtpWriter(config); break;
            case PluginNameConstants.HBASE_WRITER : writer = new HbaseWriter(config); break;
            case PluginNameConstants.HDFS_WRITER : writer = new HdfsWriter(config); break;
            case PluginNameConstants.MONGODB_WRITER : writer = new MongodbWriter(config); break;
            case PluginNameConstants.ODPS_WRITER : writer = new OdpsWriter(config); break;
            case PluginNameConstants.REDIS_WRITER : writer = new RedisWriter(config); break;
            case PluginNameConstants.HIVE_WRITER : writer = new HiveWriter(config); break;
            case PluginNameConstants.KAFKA10_WRITER : writer = new Kafka10Writer(config); break;
            case PluginNameConstants.KAFKA11_WRITER : writer = new Kafka11Writer(config); break;
            case PluginNameConstants.KUDU_WRITER : writer = new KuduWriter(config); break;
            case PluginNameConstants.CLICKHOUSE_WRITER : writer = new ClickhouseWriter(config); break;
            case PluginNameConstants.POLARDB_WRITER : writer = new PolardbWriter(config); break;
            case PluginNameConstants.KAFKA_WRITER : writer = new KafkaWriter(config); break;
            case PluginNameConstants.EMQX_WRITER : writer = new EmqxWriter(config); break;
            case PluginNameConstants.DM_WRITER : writer = new DmWriter(config); break;
            case PluginNameConstants.GREENPLUM_WRITER : writer = new GreenplumWriter(config); break;
            case PluginNameConstants.PHOENIX5_WRITER : writer = new Phoenix5Writer(config); break;
            case PluginNameConstants.KINGBASE_WRITER : writer = new KingbaseWriter(config); break;
            default:throw new IllegalArgumentException("Can not find writer by name:" + writerName);
        }

        return writer;
    }

    private static void openCheckpointConf(StreamExecutionEnvironment env, Properties properties){
        if(properties!=null){
            String interval = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY);
            if(StringUtils.isNotBlank(interval)){
                env.enableCheckpointing(Long.parseLong(interval.trim()));
                LOG.info("Open checkpoint with interval:" + interval);
            }
            String checkpointTimeoutStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY);
            if(checkpointTimeoutStr != null){
                long checkpointTimeout = Long.parseLong(checkpointTimeoutStr.trim());
                //checkpoints have to complete within one min,or are discard
                env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

                LOG.info("Set checkpoint timeout:" + checkpointTimeout);
            }
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
    }
}
