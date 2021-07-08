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


package com.dtstack.flinkx.kudu.writer;

import com.dtstack.flinkx.kudu.core.KuduConfig;
import com.dtstack.flinkx.outputformat.RichOutputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;

/**
 * @author jiangbo
 * @date 2019/7/31
 */
public class KuduOutputFormatBuilder extends RichOutputFormatBuilder {

    private KuduOutputFormat format;

    public KuduOutputFormatBuilder() {
        super.format = format = new KuduOutputFormat();
    }

    public void setColumns(List<MetaColumn> columns){
        format.columns = columns;
    }

    public void setKuduConfig(KuduConfig kuduConfig){
        format.kuduConfig = kuduConfig;
    }

    public void setWriteMode(String writeMode){
        format.writeMode = writeMode;
    }

    @Override
    protected void checkFormat() {
        if (format.columns == null || format.columns.size() == 0){
            throw new IllegalArgumentException("columns can not be empty");
        }
    }
}
