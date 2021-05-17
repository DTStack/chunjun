/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.oracle9.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.oracle9.Oracle9DatabaseMeta;
import com.dtstack.flinkx.oracle9.format.Oracle9OutputFormat;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.commons.lang3.StringUtils;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:31
 */
public class Oracle9Writer extends JdbcDataWriter {
    protected  String schema;
    public Oracle9Writer(DataTransferConfig config) {
        super(config);
        schema = config.getJob().getContent().get(0).getWriter().getParameter().getConnection().get(0).getSchema();
        if(StringUtils.isNotBlank(schema)){
            table = schema + ConstantValue.POINT_SYMBOL + table;
        }
        setDatabaseInterface(new Oracle9DatabaseMeta());
    }

    @Override
    protected JdbcOutputFormatBuilder getBuilder() {
        JdbcOutputFormatBuilder jdbcOutputFormatBuilder = new JdbcOutputFormatBuilder(new Oracle9OutputFormat());
        jdbcOutputFormatBuilder.setSchema(schema);
        return jdbcOutputFormatBuilder;
    }
}
