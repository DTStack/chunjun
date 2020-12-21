/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadataphoenix.reader;


import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.metadata.inputformat.MetadataInputFormatBuilder;
import com.dtstack.flinkx.metadata.reader.MetadataReader;
import com.dtstack.flinkx.metadataphoenix.inputformat.MetadataphoenixInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.dtstack.flinkx.metadataphoenix.util.PhoenixMetadataCons.DRIVER_NAME;

/**
 * @author kunni@dtstack.com
 */
public class MetadataphoenixReader extends MetadataReader {

    public MetadataphoenixReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        driverName = DRIVER_NAME;
    }

    @Override
    protected MetadataInputFormatBuilder getBuilder(){
        return new MetadataInputFormatBuilder(new MetadataphoenixInputFormat());
    }
}
