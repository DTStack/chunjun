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

package com.dtstack.chunjun.connector.s3.config;

import com.dtstack.chunjun.config.CommonConfig;

import com.amazonaws.regions.Regions;
import lombok.Getter;
import lombok.Setter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class S3Config extends CommonConfig implements Serializable {

    private static final long serialVersionUID = 9008329384464201903L;

    private String accessKey;

    private String secretKey;

    private String region = Regions.CN_NORTH_1.getName();

    private String endpoint;

    private String bucket;

    private List<String> objects;

    private String object;

    private char fieldDelimiter = ',';

    private String writeMode = "overwrite";

    private String encoding = "UTF-8";

    private boolean isFirstLineHeader = false;

    private long maxFileSize = 1024 * 1024L;

    private String Protocol = "HTTP";

    /**
     * Limit the number of files obtained per request. If the number of files is greater than
     * fetchSize, then read in a loop
     */
    private int fetchSize = 512;

    /** Use v2 or v1 api to get directory files */
    private boolean useV2 = true;
    /**
     * Safety caution to prevent the parser from using large amounts of memory in the case where
     * parsing settings like file encodings don't end up matching the actual format of a file. This
     * switch can be turned off if the file format is known and tested. With the switch off, the max
     * column lengths and max column count per record supported by the parser will greatly increase.
     * Default is false.
     */
    private boolean safetySwitch = false;
}
