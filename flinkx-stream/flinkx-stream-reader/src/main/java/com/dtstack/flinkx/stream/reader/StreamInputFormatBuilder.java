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

package com.dtstack.flinkx.stream.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;

/**
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class StreamInputFormatBuilder extends BaseRichInputFormatBuilder {

    private StreamInputFormat format;

    public StreamInputFormatBuilder() {
        super.format = format = new StreamInputFormat();
    }

    public void setSliceRecordCount(List<Long> sliceRecordCount){
        format.sliceRecordCount = sliceRecordCount;
    }

    public void setColumns(List<MetaColumn> columns){
        format.columns = columns;
    }

    @Override
    protected void checkFormat() {
        if (format.columns == null || format.columns.size() == 0){
            throw new IllegalArgumentException("columns can not be empty");
        }
    }
}
