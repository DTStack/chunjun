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
import com.dtstack.chunjun.mapping.MappingConfig;

import lombok.Data;

import java.io.Serializable;

@Data
public class ContentConfig implements Serializable {

    private static final long serialVersionUID = -4879980246136110446L;

    private OperatorConfig reader;

    private OperatorConfig writer;

    private TransformerConfig transformer;

    private MappingConfig nameMapping;

    /** cdc restore conf */
    private CdcConfig restoration = new CdcConfig();
}
