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

package com.dtstack.chunjun.connector.saphana.source;

import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;

import org.apache.flink.core.io.InputSplit;

import java.util.Properties;

public class SaphanaInputFormat extends JdbcInputFormat {

    private static final long serialVersionUID = -1068852381723024237L;

    @Override
    public void openInternal(InputSplit inputSplit) {
        Properties properties = jdbcConfig.getProperties();
        properties.setProperty("zeroDateTimeBehavior", "convertToNull");
        super.openInternal(inputSplit);
    }
}
