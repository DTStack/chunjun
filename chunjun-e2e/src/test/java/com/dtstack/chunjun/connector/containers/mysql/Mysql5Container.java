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

package com.dtstack.chunjun.connector.containers.mysql;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class Mysql5Container extends MysqlBaseContainer {
    private static final URL MYSQL5_DOCKERFILE =
            MysqlBaseContainer.class.getClassLoader().getResource("docker/mysql/Mysql5Dockerfile");

    private static final String MYSQL5_HOST = "chunjun-e2e-mysql5";

    public Mysql5Container() throws URISyntaxException {
        super(MYSQL5_HOST, Paths.get(MYSQL5_DOCKERFILE.toURI()));
    }
}
