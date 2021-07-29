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

package com.dtstack.flinkx.db2.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.db2.Db2DatabaseMeta;
import com.dtstack.flinkx.db2.format.Db2OutputFormat;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;

/**
 * db2 writer plugin
 *
 * @author jiangbo
 * @date 2018/11/19
 */
public class Db2Writer extends JdbcDataWriter {

    public Db2Writer(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new Db2DatabaseMeta());
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new Db2OutputFormat());
    }
}
