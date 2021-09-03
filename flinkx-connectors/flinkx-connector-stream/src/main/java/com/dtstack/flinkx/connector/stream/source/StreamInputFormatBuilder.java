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

package com.dtstack.flinkx.connector.stream.source;

import com.dtstack.flinkx.connector.stream.conf.StreamConf;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;

import org.apache.commons.collections.CollectionUtils;

/**
 * @Company: www.dtstack.com
 *
 * @author jiangbo
 */
public class StreamInputFormatBuilder extends BaseRichInputFormatBuilder {

    private final StreamInputFormat format;

    public StreamInputFormatBuilder() {
        super.format = format = new StreamInputFormat();
    }

    public void setStreamConf(StreamConf streamConf) {
        super.setConfig(streamConf);
        format.setStreamConf(streamConf);
    }

    @Override
    protected void checkFormat() {
        if (CollectionUtils.isEmpty(format.getStreamConf().getColumn())) {
            throw new IllegalArgumentException("columns can not be empty");
        }
    }
}
