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

package com.dtstack.flinkx.stream.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;

/**
 * The builder of StreamOutputFormat
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class StreamOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private StreamOutputFormat format;

    public StreamOutputFormatBuilder() {
        super.format = format = new StreamOutputFormat();
    }

    public void setPrint(boolean print){
        format.print = print;
    }

    public void setMetaColumn(List<MetaColumn> metaColumns) {
        format.metaColumns = metaColumns;
    }

    public void setWriteDelimiter(String writeDelimiter) {
        format.writeDelimiter = writeDelimiter;
    }

    @Override
    protected void checkFormat() {

    }
}
