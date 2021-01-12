/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with format work for additional information
 * regarding copyright ownership.  The ASF licenses format file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use format file except in compliance
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

package com.dtstack.flinkx.pgwal.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Date: 2019/12/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PgWalInputFormatBuilder extends BaseRichInputFormatBuilder {

    protected PgWalInputFormat format;

    public PgWalInputFormatBuilder() {
        super.format = this.format = new PgWalInputFormat();
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setUrl(String url) {
        format.url = url;
    }

    public void setDatabaseName(String databaseName) {
        format.databaseName = databaseName;
    }

    public void setPavingData(boolean pavingData) {
        format.pavingData = pavingData;
    }

    public void setTableList(List<String> tableList) {
        format.tableList = tableList;
    }

    public void setCat(String cat) {
        format.cat = cat;
    }

    public void setStatusInterval(Integer statusInterval) {
        format.statusInterval = statusInterval;
    }

    public void setLsn(Long lsn) {
        format.lsn = lsn;
    }

    public void setAllowCreateSlot(Boolean allowCreateSlot) {
        format.allowCreateSlot = allowCreateSlot;
    }

    public void setSlotName(String slotName) {
        format.slotName = slotName;
    }

    public void setTemporary(Boolean temporary) {
        format.temporary = temporary;
    }

    @Override
    protected void checkFormat() {
        if (StringUtils.isBlank(format.username)) {
            throw new IllegalArgumentException("No username supplied");
        }
        if (StringUtils.isBlank(format.password)) {
            throw new IllegalArgumentException("No password supplied");
        }
        if (StringUtils.isBlank(format.url)) {
            throw new IllegalArgumentException("No url supplied");
        }
        if (StringUtils.isBlank(format.databaseName)) {
            throw new IllegalArgumentException("No databaseName supplied");
        }
        if (CollectionUtils.isEmpty(format.tableList)) {
            throw new IllegalArgumentException("No tableList supplied");
        }
        if (StringUtils.isBlank(format.cat)) {
            throw new IllegalArgumentException("No cat supplied");
        }
        if(!format.allowCreateSlot && StringUtils.isBlank(format.slotName)){
            throw new IllegalArgumentException("slotName can not be null if allowCreateSlot is false");
        }
    }
}
