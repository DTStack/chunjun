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


package com.dtstack.flinkx.carbondata.writer.recordwriter;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

/**
 * Factory generating record writer
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class RecordWriterFactory {

    public static AbstractRecordWriter getAssembleInstance(CarbonTable carbonTable, String partition) {
        if(carbonTable.isHivePartitionTable()) {
            return new HivePartitionRecordWriter(carbonTable, partition);
        } else if(carbonTable.getPartitionInfo() == null) {
            return new SimpleRecordWriter(carbonTable);
        } else {
            return new CarbonPartitionRecordWriter(carbonTable);
        }
    }

}
