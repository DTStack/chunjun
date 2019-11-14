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
package com.dtstack.flinkx.polardb.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.mysql.writer.MysqlWriter;
import com.dtstack.flinkx.polardbd.PolardbDatabaseMeta;

/**
 * Date: 2019/11/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PolardbWriter extends MysqlWriter {
    public PolardbWriter(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new PolardbDatabaseMeta());
    }
}
