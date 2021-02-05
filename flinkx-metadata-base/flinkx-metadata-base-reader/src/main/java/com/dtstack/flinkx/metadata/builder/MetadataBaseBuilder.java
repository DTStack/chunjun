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

package com.dtstack.flinkx.metadata.builder;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputFormat;

import java.util.List;
import java.util.Map;

/**
 * @author kunni@dtstack.com
 */
public class MetadataBaseBuilder extends BaseRichInputFormatBuilder {


    protected MetadataBaseInputFormat format;

    public MetadataBaseBuilder(MetadataBaseInputFormat format){
        super.format = this.format = format;
    }

    public void setOriginalJob(List<Map<String, Object>> originalJob){
        format.setOriginalJob(originalJob);
    }

    /**
     * 校验不同插件所必须的参数
     */
    @Override
    protected void checkFormat() {

    }
}
