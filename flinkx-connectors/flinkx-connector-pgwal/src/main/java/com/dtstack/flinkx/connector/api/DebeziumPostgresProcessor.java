/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.api;

import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.OffsetStorageReaderWrapper;
import io.debezium.connector.common.SourceRecordWrapper;
import io.debezium.connector.common.SourceTaskContextWrapper;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorTask;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

//public class DebeziumPostgresProcessor extends DatabaseBaseRichInputFormat<RowData, RowData, SourceRecordWrapper>
//        implements ServiceProcessor<RowData, RowData> {
//
//    private static final Logger LOG = LoggerFactory.getLogger(DebeziumPostgresProcessor.class);
//
//    private volatile boolean running;
//
//    private PostgresConnectorTask<SourceTaskContextWrapper, SourceRecordWrapper, Object> postgresConnectorTask;
//    private DebeziumPostgresDataProcessor processor;
//    private BlockingQueue<RowData> queue;
//
//    public DebeziumPostgresProcessor() {
//        this(new HashMap<>());
//    }
//
//    public DebeziumPostgresProcessor(Map<String, Object> config) {
//        super(config);
//        this.queue = new ArrayBlockingQueue<>(2 << 5);
//    }
//
//    @Override
//    protected void openInternal(InputSplit inputSplit) throws IOException {
//        super.openInternal(inputSplit);
//        LOG.info("init the param");
//        try {
//            init(getParams());
//        } catch (SQLException e) {
//            throw new IOException(e);
//        }
//        this.running = true;
//        LOG.info("start process data");
//        Executors.newFixedThreadPool(1).submit(() -> {
//            try {
//                process(new SimpleContext());
//            } catch (SQLException e) {
//                throw new FlinkxRuntimeException(e);
//            }
//        });
//
//    }
//
//    @Override
//    public String type() {
//        return "debezium-pg";
//    }
//
//    @Override
//    public void init(Map<String, Object> param) throws SQLException {
//        postgresConnectorTask = new PostgresConnectorTask<>();
//        postgresConnectorTask.initialize(new SourceTaskContextWrapper() {
//            @Override
//            public Map<String, String> configs() {
//                return new HashMap<>();
//            }
//
//            @Override
//            public OffsetStorageReaderWrapper offsetStorageReader() {
//                return new OffsetStorageReaderWrapper() {
//                    @Override
//                    public <T> Map<String, Object> offset(Map<String, T> partition) {
//                        return new HashMap<>();
//                    }
//
//                    @Override
//                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
//                        return new HashMap<>();
//                    }
//                };
//            }
//        });
//        String prefix = "database.";
//        JdbcConfiguration configuration = JdbcConfiguration.copy(Configuration.fromSystemProperties(""))
//                .withDefault(prefix + JdbcConfiguration.DATABASE, param.get("database"))
//                .withDefault(prefix + JdbcConfiguration.HOSTNAME, param.getOrDefault("hostname", "localhost"))
//                .withDefault(prefix + JdbcConfiguration.PORT, param.getOrDefault("port", 5432))
//                .withDefault(prefix + JdbcConfiguration.USER, param.getOrDefault("username", "postgres"))
//                .withDefault(prefix + JdbcConfiguration.PASSWORD, param.getOrDefault("password", "postgres"))
//                .withDefault(RelationalDatabaseConnectorConfig.SERVER_NAME, param.getOrDefault("service.name", "local"))
//                .withDefault("plugin.name", param.getOrDefault("plugin.name", PostgresConnectorConfig.LogicalDecoder.PGOUTPUT))
//                .withDefault("slot.name", param.getOrDefault("slot.name", "test01"))
//                .build();
//        postgresConnectorTask.start(configuration.asMap());
//        this.running = true;
//        LOG.info("init PGReplicationStream successfully...");
//    }
//
//    @Override
//    public void process(Context context) throws SQLException {
//        List<SourceRecordWrapper> recordWrappers;
//        while (running) {
//            try {
//                recordWrappers = postgresConnectorTask.poll();
//                if(recordWrappers != null && !recordWrappers.isEmpty()) {
//                    context.set("data", recordWrappers);
//                    dataProcessor().setConnectorConsumer((ConnectorConsumer) rowData -> {
//                        try {
//                            queue.put(rowData);
//                        } catch (InterruptedException e) {
//                            throw new FlinkxRuntimeException(e);
//                        }
//                    });
//                    dataProcessor().process(context);
//                    postgresConnectorTask.commitRecord(recordWrappers.get(recordWrappers.size() - 1));
//                }
//            } catch (IOException | InterruptedException e) {
//                dataProcessor().processException(e);
//            }
//        }
//    }
//
//    @Override
//    protected RowData nextRecordInternal(RowData rowData) throws IOException {
//        return queue.poll();
//    }
//
//    @Override
//    public DataProcessor<RowData> dataProcessor() {
//        if(processor != null) {
//            return processor;
//        }
//        processor =  new DebeziumPostgresDataProcessor(getParams());
//        return processor;
//    }
//
//    @Override
//    public void close() throws IOException {
//        postgresConnectorTask.stop();
//    }
//}
