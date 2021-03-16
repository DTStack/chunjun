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

package com.dtstack.flinkx.metadataphoenix5.inputformat;

import com.dtstack.metadata.rdb.builder.MetadatardbBuilder;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author kunni@dtstack.com
 */
public class MetadataPhoenixBuilder extends MetadatardbBuilder {

    protected Metadataphoenix5InputFormat format;

    public MetadataPhoenixBuilder(Metadataphoenix5InputFormat format) {
        super(format);
        this.format = format;
    }

    @Override
    protected void checkFormat() {
        super.checkFormat();
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isEmpty(format.path)) {
            sb.append("phoenix zookeeper can not be empty ;\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public void setPath(String path){
        format.setPath(path);
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig){
        format.setHadoopConfig(hadoopConfig);
    }
}
