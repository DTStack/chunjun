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
package com.dtstack.chunjun.connector.jdbc.sink.wrapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface JdbcBatchStatementWrapper<T> {

    /**
     * add statement to batch
     *
     * @param record the record to add
     */
    void addToBatch(T record) throws Exception;

    /**
     * Execute the batch.
     *
     * @throws SQLException if the batch could not be executed
     */
    void executeBatch() throws Exception;

    void writeSingleRecord(T record) throws Exception;

    ResultSet executeQuery(T record) throws Exception;

    /**
     * Clear parameters.
     *
     * @throws SQLException if the parameters could not be cleared
     */
    void clearParameters() throws SQLException;

    /**
     * Close the statement.
     *
     * @throws SQLException if the statement could not be closed
     */
    void close() throws SQLException;

    /**
     * Clear the batch.
     *
     * @throws SQLException if the batch could not be cleared
     */
    void clearBatch() throws SQLException;

    /**
     * Reopen the statement.
     *
     * @throws SQLException if the statement could not be reopened
     */
    void reOpen(Connection connection) throws SQLException;

    /** Clear {@link JdbcBatchStatementWrapper} cache if exists. */
    void clearStatementCache();
}
