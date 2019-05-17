/**
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

package com.dtstack.flinkx.inputformat;

import com.dtstack.flinkx.config.RestoreConfig;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract specification for all the InputFormatBuilder implementation
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class RichInputFormatBuilder {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected RichInputFormat format;

    public void setMonitorUrls(String monitorUrls) {
        format.monitorUrls = monitorUrls;
    }

    public void setBytes(long bytes) {
        format.bytes = bytes;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig){
        format.restoreConfig = restoreConfig;
    }

    protected abstract void checkFormat();

    public RichInputFormat finish() {
        Preconditions.checkNotNull(format);
        checkFormat();
        return format;
    }

}
