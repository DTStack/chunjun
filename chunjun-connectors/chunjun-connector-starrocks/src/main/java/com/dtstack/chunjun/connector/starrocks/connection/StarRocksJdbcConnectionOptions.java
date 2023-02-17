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

package com.dtstack.chunjun.connector.starrocks.connection;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Optional;

import static com.dtstack.chunjun.connector.starrocks.options.ConstantValue.CJ_DRIVER_CLASS_NAME;
import static com.dtstack.chunjun.connector.starrocks.options.ConstantValue.DRIVER_CLASS_NAME;

/** JDBC connection options. */
public class StarRocksJdbcConnectionOptions implements Serializable {

    private static final long serialVersionUID = 6052520834649527341L;
    protected final String url;
    protected final String username;
    protected final String password;

    public StarRocksJdbcConnectionOptions(String url, String username, String password) {
        this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
        this.username = username;
        this.password = password;
    }

    public String getDbURL() {
        return url;
    }

    public String getCjDriverName() {
        return CJ_DRIVER_CLASS_NAME;
    }

    public String getDriverName() {
        return DRIVER_CLASS_NAME;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }
}
