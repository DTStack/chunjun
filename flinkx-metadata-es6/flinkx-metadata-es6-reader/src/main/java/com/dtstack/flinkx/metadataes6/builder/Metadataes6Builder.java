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
package com.dtstack.flinkx.metadataes6.builder;


import com.dtstack.flinkx.metadata.builder.MetadataBaseBuilder;
import com.dtstack.flinkx.metadataes6.format.Metadataes6InputFormat;

/**
 * @author : baiyu
 * @date : 2020/12/30
 */
public class Metadataes6Builder extends MetadataBaseBuilder {

    private Metadataes6InputFormat format;

    public Metadataes6Builder(Metadataes6InputFormat format) {
        super(format);
        this.format = format;
    }

    public void setUsername(String username){
        format.setUsername(username);
    }

    public void setPassword(String password){
        format.setPassword(password);
    }

    public void setUrl(String url){
        format.setUrl(url);
    }
}
