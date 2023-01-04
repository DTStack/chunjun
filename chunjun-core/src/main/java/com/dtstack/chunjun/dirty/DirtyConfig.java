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

package com.dtstack.chunjun.dirty;

import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.throwable.NoRestartException;

import lombok.Data;

import java.io.Serializable;
import java.util.Properties;

@Data
public class DirtyConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * This is the limit on the max consumed-data. The consumer would to be killed with throwing a
     * {@link NoRestartException} when the consumed-count exceed the limit.
     */
    protected long maxConsumed;

    /** This is the limit on the max failed-consumed-data. Same as {@link #maxConsumed} */
    protected long maxFailedConsumed;

    /** The type of dirty-plugin. */
    private String type;

    /** Print dirty-data every ${printRate}. */
    private Long printRate = 1L;

    /** Custom parameters of different dirty-plugin. */
    private Properties pluginProperties = new Properties();

    /** ChunJun dirty-plugins local plugins path {@link Options#getFlinkLibDir()} */
    private String localPluginPath;
}
