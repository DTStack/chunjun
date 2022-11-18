/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.dirty.mysql;

import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.consumer.DirtyDataCollector;
import com.dtstack.chunjun.dirty.impl.DirtyDataEntry;
import com.dtstack.chunjun.factory.ChunJunThreadFactory;
import com.dtstack.chunjun.throwable.NoRestartException;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class MysqlDirtyDataCollector extends DirtyDataCollector {
    private static final long serialVersionUID = -6019792640886034069L;

    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";

    private static final String[] TABLE_FIELDS = {
        "job_id",
        "job_name",
        "operator_name",
        "dirty_data",
        "error_message",
        "field_name",
        "create_time"
    };

    private static final int CONN_VALID_TIME = 1000;

    private final List<DirtyDataEntry> entities = new LinkedList<>();

    /** For schedule task of flush. */
    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture<?> scheduledFuture;

    private transient PreparedStatement statement;

    private transient Connection connection;

    private String url;

    private String username;

    private String password;

    private String schemaInfo;

    private Long batchIntervalMill;

    private Long batchSize;

    private String quoteIdentifier(String tableName) {
        return "`" + tableName + "`";
    }

    /**
     * Do something with mysql configuration before consume.
     *
     * @param url mysql url.
     * @param userName mysql username.
     * @param password mysql password.
     * @param tableName mysql table name.
     * @throws ClassNotFoundException thrown if mysql driver is unavailable.
     * @throws SQLException thrown if you get connection fails.
     */
    private void beforeConsume(String url, String userName, String password, String tableName)
            throws ClassNotFoundException, SQLException {
        synchronized (this) {
            Class.forName(DRIVER_NAME);
        }

        connection = DriverManager.getConnection(url, userName, password);
        statement = prepareStatement(connection, tableName);
    }

    /**
     * Prepare statement with mysql configuration.
     *
     * @param connection mysql connection.
     * @param tableName dirty table name.
     * @return prepared statement.
     * @throws SQLException exception when prepare statement fails.
     */
    private PreparedStatement prepareStatement(Connection connection, String tableName)
            throws SQLException {
        String placeholders =
                Arrays.stream(TABLE_FIELDS).map(f -> "?").collect(Collectors.joining(", "));

        String insertField =
                Arrays.stream(TABLE_FIELDS)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertSql =
                "INSERT INTO " + tableName + " (" + insertField + ") VALUES (" + placeholders + ")";
        return connection.prepareStatement(insertSql);
    }

    /**
     * Initialize the timed flush-task.
     *
     * @param batchWaitInterval the time interval for flush-task.
     */
    private void initScheduledTask(Long batchWaitInterval) {
        try {
            if (batchWaitInterval > 0) {
                log.info("begin to init ScheduledTask");
                this.scheduler =
                        new ScheduledThreadPoolExecutor(
                                1,
                                new ChunJunThreadFactory(
                                        "mysql-dirty-batch-flusher",
                                        true,
                                        (t, e) -> {
                                            log.error("mysql-dirty-batch-flusher occur error!", e);
                                        }));

                this.scheduledFuture =
                        this.scheduler.scheduleWithFixedDelay(
                                () -> {
                                    try {
                                        synchronized (this) {
                                            log.debug("ready to add records");
                                            if (!entities.isEmpty()) {
                                                log.debug(
                                                        "ready to add records,record num is "
                                                                + entities.size());
                                                flush();
                                                log.debug("end add records");
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.error("ScheduledTask add record error", e);
                                    }
                                },
                                batchWaitInterval,
                                batchWaitInterval,
                                TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            throw new RuntimeException("init schedule task failed !", e);
        }
    }

    @Override
    public void open() {
        initScheduledTask(batchIntervalMill);

        try {
            beforeConsume(url, username, password, schemaInfo);
        } catch (SQLException | ClassNotFoundException e) {
            throw new NoRestartException("Open mysql-dirty-consumer failed!", e);
        }
    }

    @Override
    protected void init(DirtyConfig conf) {
        Properties mysqlProperties = conf.getPluginProperties();
        Object userName = mysqlProperties.get("jdbc.username");
        Object password = mysqlProperties.get("jdbc.password");
        Object url = mysqlProperties.get("jdbc.url");
        Object database = mysqlProperties.get("jdbc.database");
        Object table = mysqlProperties.get("jdbc.table");
        Object batchSize = mysqlProperties.getOrDefault("jdbc.batch-size", 1000L);
        Object batchInterval = mysqlProperties.getOrDefault("jdbc.batch-flush-interval", 1000L);

        checkNotNull(userName, "jdbc.username can not be null.");
        checkNotNull(url, "jdbc.url can not be null.");
        checkNotNull(table, "jdbc.table can not be null.");

        this.batchSize = Long.parseLong(String.valueOf(batchSize));
        this.batchIntervalMill = Long.parseLong(String.valueOf(batchInterval));
        this.username = String.valueOf(userName);
        this.password = String.valueOf(password);
        this.schemaInfo = schemaInfo(database, table);
        this.url = String.valueOf(url);
    }

    private String schemaInfo(Object database, Object table) {
        return database == null
                ? quoteIdentifier(String.valueOf(table))
                : quoteIdentifier(String.valueOf(database))
                        + "."
                        + quoteIdentifier(String.valueOf(table));
    }

    /** execute statement with single execution. */
    private void singleFlush() {
        for (DirtyDataEntry item : entities) {
            try {
                final String[] dirtyArrays = item.toArray();
                for (int i = 0; i < TABLE_FIELDS.length; i++) {
                    statement.setObject(i + 1, dirtyArrays[i]);
                }
                statement.execute();
            } catch (Exception e) {
                addFailedConsumed(e, 1L);
            }
        }
    }

    /**
     * execute statement with batch execution. If exceptions failed, it will execute with single
     * exception.
     */
    private void flush() {
        try {
            for (DirtyDataEntry item : entities) {
                final String[] dirtyArrays = item.toArray();
                for (int i = 0; i < TABLE_FIELDS.length; i++) {
                    statement.setObject(i + 1, dirtyArrays[i]);
                }
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            singleFlush();
        } finally {
            entities.clear();
        }
    }

    /**
     * Sink dirty to mysql datasource.
     *
     * @param dirty dirty-data which should be consumed.
     * @throws Exception thrown exception when consume data fails.
     */
    @Override
    protected void consume(DirtyDataEntry dirty) throws Exception {
        entities.add(dirty);

        if (consumedCounter.getLocalValue() % batchSize == 0) {
            flush();
        }
    }

    @Override
    public void close() {
        isRunning.compareAndSet(true, false);

        if (!entities.isEmpty()) {
            flush();
        }

        try {
            if (connection != null && !connection.isValid(CONN_VALID_TIME)) {
                connection.close();
            }
        } catch (SQLException e) {
            log.warn("Close mysql connection failed!", e);
        }

        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (SQLException e) {
            log.warn("Close mysql statement failed!", e);
        }

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        }
    }
}
