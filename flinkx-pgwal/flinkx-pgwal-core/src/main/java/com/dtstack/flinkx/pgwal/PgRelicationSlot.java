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
package com.dtstack.flinkx.pgwal;

/**
 * Date: 2019/12/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PgRelicationSlot {
    private String slotName;
    private String plugin;
    private String slotType;
    private Integer datoid;
    private String database;
    private String temporary;
    private String active;
    private Integer activePid;
    private String xmin;
    private String catalogXmin;
    private String restartLsn;
    private String confirmedFlushLsn;

    public boolean isActive(){
        return "t".equalsIgnoreCase(active);
    }

    public boolean isNotActive(){
        return !isActive();
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public String getSlotType() {
        return slotType;
    }

    public void setSlotType(String slotType) {
        this.slotType = slotType;
    }

    public Integer getDatoid() {
        return datoid;
    }

    public void setDatoid(Integer datoid) {
        this.datoid = datoid;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTemporary() {
        return temporary;
    }

    public void setTemporary(String temporary) {
        this.temporary = temporary;
    }

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
    }

    public Integer getActivePid() {
        return activePid;
    }

    public void setActivePid(Integer activePid) {
        this.activePid = activePid;
    }

    public String getXmin() {
        return xmin;
    }

    public void setXmin(String xmin) {
        this.xmin = xmin;
    }

    public String getCatalogXmin() {
        return catalogXmin;
    }

    public void setCatalogXmin(String catalogXmin) {
        this.catalogXmin = catalogXmin;
    }

    public String getRestartLsn() {
        return restartLsn;
    }

    public void setRestartLsn(String restartLsn) {
        this.restartLsn = restartLsn;
    }

    public String getConfirmedFlushLsn() {
        return confirmedFlushLsn;
    }

    public void setConfirmedFlushLsn(String confirmedFlushLsn) {
        this.confirmedFlushLsn = confirmedFlushLsn;
    }

    @Override
    public String toString() {
        return "PgRelicationSlots{" +
                "slotName='" + slotName + '\'' +
                ", plugin='" + plugin + '\'' +
                ", slotType='" + slotType + '\'' +
                ", datoid=" + datoid +
                ", database='" + database + '\'' +
                ", temporary='" + temporary + '\'' +
                ", active='" + active + '\'' +
                ", activePid='" + activePid + '\'' +
                ", xmin='" + xmin + '\'' +
                ", catalogXmin='" + catalogXmin + '\'' +
                ", restartLsn='" + restartLsn + '\'' +
                ", conFirmedFlushLsn='" + confirmedFlushLsn + '\'' +
                '}';
    }
}
