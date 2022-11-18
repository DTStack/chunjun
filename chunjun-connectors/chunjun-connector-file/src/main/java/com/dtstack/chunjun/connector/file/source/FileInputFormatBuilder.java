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

package com.dtstack.chunjun.connector.file.source;

import com.dtstack.chunjun.config.BaseFileConf;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.commons.lang3.StringUtils;

/**
 * @program: ChunJun
 * @author: xiuzhu
 * @create: 2021/06/24
 */
public class FileInputFormatBuilder extends BaseRichInputFormatBuilder<FileInputFormat> {

    public FileInputFormatBuilder() {
        super(new FileInputFormat());
    }

    public void setFileConf(BaseFileConf fileConf) {
        super.setConfig(fileConf);
        format.setFileConf(fileConf);
    }

    @Override
    protected void checkFormat() {

        BaseFileConf fileConf = format.getFileConf();
        if (StringUtils.isBlank(fileConf.getPath())) {
            throw new IllegalArgumentException("file path cannot be blank");
        }
    }
}
