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
import com.dtstack.flinkx.binlog.reader.BinlogReader;
import com.dtstack.flinkx.carbondata.reader.CarbondataReader;
import com.dtstack.flinkx.clickhouse.reader.ClickhouseReader;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.db2.reader.Db2Reader;
import com.dtstack.flinkx.es.reader.EsReader;
import com.dtstack.flinkx.ftp.reader.FtpReader;
import com.dtstack.flinkx.gbase.reader.GbaseReader;
import com.dtstack.flinkx.hbase.reader.HbaseReader;
import com.dtstack.flinkx.hdfs.reader.HdfsReader;
import com.dtstack.flinkx.kafka.reader.KafkaReader;
import com.dtstack.flinkx.kafka09.reader.Kafka09Reader;
import com.dtstack.flinkx.kafka10.reader.Kafka10Reader;
import com.dtstack.flinkx.kafka11.reader.Kafka11Reader;
import com.dtstack.flinkx.kudu.reader.KuduReader;
import com.dtstack.flinkx.mongodb.reader.MongodbReader;
import com.dtstack.flinkx.mysql.reader.MysqlReader;
import com.dtstack.flinkx.mysqld.reader.MysqldReader;
import com.dtstack.flinkx.odps.reader.OdpsReader;
import com.dtstack.flinkx.oracle.reader.OracleReader;
import com.dtstack.flinkx.oraclelogminer.reader.OraclelogminerReader;
import com.dtstack.flinkx.polardb.reader.PolardbReader;
import com.dtstack.flinkx.postgresql.reader.PostgresqlReader;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.sqlserver.reader.SqlserverReader;
import com.dtstack.flinkx.stream.reader.StreamReader;
import com.dtstack.flinkx.test.PluginNameConstrant;
import com.dtstack.flinkx.test.core.result.ReaderResult;
import com.dtstack.flinkx.test.core.result.BaseResult;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author jiangbo
 */
public class ReaderUnitTestLauncher extends UnitTestLauncher {

    private static List<Row> resultData;

    public ReaderUnitTestLauncher() {
        resultData = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public String pluginType() {
        return "reader";
    }

    @Override
    public BaseResult runJob() throws Exception{
        String job = readJob();

        for (ParameterReplace parameterReplace : parameterReplaces) {
            job = parameterReplace.replaceParameter(job);
        }

        job = replaceChannel(job);

        DataTransferConfig config = DataTransferConfig.parse(job);
        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();

        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);

        openCheckpointConf(env);

        env.setParallelism(speedConfig.getChannel());
        env.setRestartStrategy(RestartStrategies.noRestart());

        BaseDataReader reader = buildDataReader(config, env);
        DataStream<Row> dataStream = reader.readData();

        dataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                resultData.add(value);
            }
        });

        if(StringUtils.isNotEmpty(savepointPath)){
            env.setSettings(SavepointRestoreSettings.forPath(savepointPath));
        }

        JobExecutionResult executionResult = env.execute();
        return new ReaderResult(executionResult, resultData);
    }

    private static BaseDataReader buildDataReader(DataTransferConfig config, StreamExecutionEnvironment env){
        String readerName = config.getJob().getContent().get(0).getReader().getName();
        BaseDataReader reader ;
        switch (readerName){
            case PluginNameConstrant.STREAM_READER : reader = new StreamReader(config, env); break;
            case PluginNameConstrant.CARBONDATA_READER : reader = new CarbondataReader(config, env); break;
            case PluginNameConstrant.ORACLE_READER : reader = new OracleReader(config, env); break;
            case PluginNameConstrant.POSTGRESQL_READER : reader = new PostgresqlReader(config, env); break;
            case PluginNameConstrant.SQLSERVER_READER : reader = new SqlserverReader(config, env); break;
            case PluginNameConstrant.MYSQLD_READER : reader = new MysqldReader(config, env); break;
            case PluginNameConstrant.MYSQL_READER : reader = new MysqlReader(config, env); break;
            case PluginNameConstrant.DB2_READER : reader = new Db2Reader(config, env); break;
            case PluginNameConstrant.GBASE_READER : reader = new GbaseReader(config, env); break;
            case PluginNameConstrant.ES_READER : reader = new EsReader(config, env); break;
            case PluginNameConstrant.FTP_READER : reader = new FtpReader(config, env); break;
            case PluginNameConstrant.HBASE_READER : reader = new HbaseReader(config, env); break;
            case PluginNameConstrant.HDFS_READER : reader = new HdfsReader(config, env); break;
            case PluginNameConstrant.MONGODB_READER : reader = new MongodbReader(config, env); break;
            case PluginNameConstrant.ODPS_READER : reader = new OdpsReader(config, env); break;
            case PluginNameConstrant.BINLOG_READER : reader = new BinlogReader(config, env); break;
            case PluginNameConstrant.KAFKA09_READER : reader = new Kafka09Reader(config, env); break;
            case PluginNameConstrant.KAFKA10_READER : reader = new Kafka10Reader(config, env); break;
            case PluginNameConstrant.KAFKA11_READER : reader = new Kafka11Reader(config, env); break;
            case PluginNameConstrant.KAFKA_READER : reader = new KafkaReader(config, env); break;
            case PluginNameConstrant.KUDU_READER : reader = new KuduReader(config, env); break;
            case PluginNameConstrant.CLICKHOUSE_READER : reader = new ClickhouseReader(config, env); break;
            case PluginNameConstrant.POLARDB_READER : reader = new PolardbReader(config, env); break;
            case PluginNameConstrant.ORACLE_LOG_MINER_READER : reader = new OraclelogminerReader(config, env); break;
            default:throw new IllegalArgumentException("Can not find reader by name:" + readerName);
        }

        return reader;
    }
}
