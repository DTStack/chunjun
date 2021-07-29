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

package com.dtstack.flinkx.teradata.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.teradata.TeradataDatabaseMeta;
import com.dtstack.flinkx.teradata.format.TeradataOutputFormat;

/**
 * Teradata writer plugin
 *
 * Company: www.dtstack.com
 * @author wuhui
 */
public class TeradataWriter extends JdbcDataWriter {

    public TeradataWriter(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new TeradataDatabaseMeta());
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        return new JdbcOutputFormatBuilder(new TeradataOutputFormat());
    }
}
