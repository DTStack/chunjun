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
package com.dtstack.chunjun.connector.binlog.inputformat;

import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;

/** @author liuliu 2021/12/30 */
public class UpdrdbController extends Thread {

    private MysqlEventParser controller;
    private String usernameAndGroup;

    public UpdrdbController(MysqlEventParser controller, String usernameAndGroup) {
        this.controller = controller;
        this.usernameAndGroup = usernameAndGroup;
    }

    @Override
    public void run() {
        controller.start();
    }

    public MysqlEventParser getController() {
        return controller;
    }

    public void setController(MysqlEventParser controller) {
        this.controller = controller;
    }

    public String getUsernameAndGroup() {
        return usernameAndGroup;
    }

    public void setUsernameAndGroup(String usernameAndGroup) {
        this.usernameAndGroup = usernameAndGroup;
    }

    @Override
    public String toString() {
        return "UpdrdbController{"
                + "controller="
                + controller
                + ", usernameAndGroup='"
                + usernameAndGroup
                + '\''
                + '}';
    }
}
