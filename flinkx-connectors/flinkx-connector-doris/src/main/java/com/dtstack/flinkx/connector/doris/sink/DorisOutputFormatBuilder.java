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

package com.dtstack.flinkx.connector.doris.sink;

import com.dtstack.flinkx.connector.doris.options.DorisConf;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormatBuilder;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/11/8
 */
public class DorisOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private final DorisOutputFormat format;

    public DorisOutputFormatBuilder() {
        super.format = format = new DorisOutputFormat();
    }

    public void setDorisOptions(DorisConf options) {
        format.setOptions(options);
        format.setConfig(options);
    }

    @Override
    protected void checkFormat() {}
}
