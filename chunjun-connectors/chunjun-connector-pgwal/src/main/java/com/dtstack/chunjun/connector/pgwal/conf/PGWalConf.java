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

package com.dtstack.chunjun.connector.pgwal.conf;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConf;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** config of pg cdc */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PGWalConf extends CommonConfig {

    public String username;
    public String password;

    @JsonProperty("url")
    public String jdbcUrl;

    public String cat;
    public boolean pavingData = true;

    @JsonProperty("tableList")
    public List<String> tables;

    @JsonProperty("databaseName")
    private String database;

    private String slotName = "";

    @JsonProperty("allowCreateSlot")
    private Boolean allowCreated = false;

    @JsonProperty("temporary")
    private Boolean isTemp = false;

    private Integer statusInterval = 10;
    private Long lsn = 0L;
    private List<FieldConf> column;
    private boolean slotAvailable;

    public void setCredentials(String username, String password) {
        Preconditions.checkArgument(
                StringUtils.isNotEmpty(username), "user name should not be empty");
        this.username = username;
        this.password = password;
    }

    public void setNamespace(String catalog, String database) {
        this.cat = catalog;
        this.database = database;
    }

    public void setSlotAttribute(String slotName, Boolean allowCreated, Boolean isTemp) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(slotName), "slot name cannot be empty");
        this.slotName = slotName;
        this.allowCreated = allowCreated;
        this.isTemp = isTemp;
    }

    public void setTableList(List<String> tables) {
        this.tables = tables;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String url) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(url), "url should not be empty");
        this.jdbcUrl = url;
    }

    public String getCat() {
        return cat;
    }

    public boolean isPavingData() {
        return pavingData;
    }

    public void setPavingData(Boolean isPaving) {
        this.pavingData = isPaving;
    }

    public List<String> getTables() {
        return tables;
    }

    public List<String> getSimpleTables() {
        if (tables == null) return new ArrayList<>();

        return tables.stream()
                .map(
                        (name -> {
                            String[] nameArray = name.split("\\.");
                            return nameArray[nameArray.length - 1];
                        }))
                .collect(Collectors.toList());
    }

    public String getDatabase() {
        return database;
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public Boolean getAllowCreated() {
        return allowCreated;
    }

    public Boolean getTemp() {
        return isTemp;
    }

    public Integer getStatusInterval() {
        return statusInterval;
    }

    public void setStatusInterval(Integer statusInterval) {
        Preconditions.checkArgument(statusInterval >= 0, "status interval is not negative");
        this.statusInterval = statusInterval;
    }

    public Long getLsn() {
        return lsn;
    }

    public void setLsn(Long lsn) {
        Preconditions.checkArgument(lsn >= 0, "lsn is not negative");
        this.lsn = lsn;
    }

    @Override
    public List<FieldConf> getColumn() {
        return column;
    }

    @Override
    public void setColumn(List<FieldConf> column) {
        this.column = column;
    }

    public boolean isSlotAvailable() {
        return slotAvailable;
    }

    public void setSlotAvailable(boolean slotAvailable) {
        this.slotAvailable = slotAvailable;
    }
}
