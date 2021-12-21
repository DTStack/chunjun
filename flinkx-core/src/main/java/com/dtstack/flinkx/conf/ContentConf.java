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
package com.dtstack.flinkx.conf;

import com.dtstack.flinkx.mapping.NameMappingConf;

import java.io.Serializable;

/**
 * Date: 2021/01/18 Company: www.dtstack.com
 *
 * @author tudou
 */
public class ContentConf implements Serializable {
    private static final long serialVersionUID = 1L;

    private OperatorConf reader;
    private OperatorConf writer;
    private TransformerConf transformer;
    private NameMappingConf nameMapping;

    public OperatorConf getReader() {
        return reader;
    }

    public void setReader(OperatorConf reader) {
        this.reader = reader;
    }

    public OperatorConf getWriter() {
        return writer;
    }

    public void setWriter(OperatorConf writer) {
        this.writer = writer;
    }

    public TransformerConf getTransformer() {
        return transformer;
    }

    public void setTransformer(TransformerConf transformer) {
        this.transformer = transformer;
    }

    public NameMappingConf getNameMapping() {
        return nameMapping;
    }

    public void setNameMapping(NameMappingConf nameMapping) {
        this.nameMapping = nameMapping;
    }

    @Override
    public String toString() {
        return "ContentConf{"
                + "reader="
                + reader
                + ", writer="
                + writer
                + ", transformer="
                + transformer
                + ", nameMapping="
                + nameMapping
                + '}';
    }
}
