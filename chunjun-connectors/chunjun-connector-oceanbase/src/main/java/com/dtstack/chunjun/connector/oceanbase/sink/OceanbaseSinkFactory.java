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
package com.dtstack.chunjun.connector.oceanbase.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcSinkFactory;
import com.dtstack.chunjun.connector.oceanbase.config.OceanBaseConf;
import com.dtstack.chunjun.connector.oceanbase.config.OceanBaseMode;
import com.dtstack.chunjun.connector.oceanbase.dialect.OceanbaseMysqlModeDialect;
import com.dtstack.chunjun.connector.oceanbase.dialect.OceanbaseOracleModeDialect;

public class OceanbaseSinkFactory extends JdbcSinkFactory {

    private OceanBaseConf oceanBaseConf;

    public OceanbaseSinkFactory(SyncConfig syncConfig) {
        super(syncConfig, new OceanbaseMysqlModeDialect());
        this.oceanBaseConf = (OceanBaseConf) this.jdbcConfig;
        if (oceanBaseConf != null) {
            OceanBaseMode mode = OceanBaseMode.valueOf(oceanBaseConf.getOceanBaseMode());
            // 若是for oracle模式
            if (mode == OceanBaseMode.ORACLE) {
                this.jdbcDialect = new OceanbaseOracleModeDialect();
            }
        }
    }

    @Override
    protected Class<? extends JdbcConfig> getConfClass() {
        return OceanBaseConf.class;
    }
}
