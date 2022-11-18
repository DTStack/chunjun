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
package com.dtstack.chunjun.config;

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.mapping.MappingConf;

import java.io.Serializable;
import java.util.StringJoiner;

public class ContentConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private OperatorConfig reader;
    private OperatorConfig writer;
    private TransformerConfig transformer;
    private MappingConf nameMapping;
    /** cdc restore conf */
    private CdcConfig restoration = new CdcConfig();

    public OperatorConfig getReader() {
        return reader;
    }

    public void setReader(OperatorConfig reader) {
        this.reader = reader;
    }

    public OperatorConfig getWriter() {
        return writer;
    }

    public void setWriter(OperatorConfig writer) {
        this.writer = writer;
    }

    public TransformerConfig getTransformer() {
        return transformer;
    }

    public void setTransformer(TransformerConfig transformer) {
        this.transformer = transformer;
    }

    public MappingConf getNameMapping() {
        return nameMapping;
    }

    public void setNameMapping(MappingConf nameMapping) {
        this.nameMapping = nameMapping;
    }

    public CdcConfig getRestoration() {
        return restoration;
    }

    public void setRestoration(CdcConfig restoration) {
        this.restoration = restoration;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ContentConfig.class.getSimpleName() + "[", "]")
                .add("reader=" + reader)
                .add("writer=" + writer)
                .add("transformer=" + transformer)
                .add("nameMapping=" + nameMapping)
                .add("restoration=" + restoration)
                .toString();
    }
}
