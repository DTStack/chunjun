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

package com.dtstack.flinkx.sqlserver.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.sqlserver.SqlServerConfigKeys;
import com.dtstack.flinkx.sqlserver.SqlServerDatabaseMeta;
import com.dtstack.flinkx.sqlserver.format.SqlserverOutputFormat;

/**
 * SQLServer writer plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class SqlserverWriter extends JdbcDataWriter {

    //是否在sql语句后面添加 with(nolock) ,默认是false
    private Boolean withNoLock;

    public SqlserverWriter(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new SqlServerDatabaseMeta());
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        withNoLock = readerConfig.getParameter().getBooleanVal(SqlServerConfigKeys.WITH_NO_LOCK,false);
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        SqlserverOutputFormat sqlserverOutputFormat = new SqlserverOutputFormat();
        sqlserverOutputFormat.setWithNoLock(withNoLock);
        return new JdbcOutputFormatBuilder(sqlserverOutputFormat);
    }


}
