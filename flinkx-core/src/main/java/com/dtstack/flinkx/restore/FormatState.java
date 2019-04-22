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

package com.dtstack.flinkx.restore;

import java.io.Serializable;

/**
 * @author jiangbo
 * @explanation
 * @date 2019/2/28
 */
public class FormatState implements Serializable {

    private int numOfSubTask;

    private Object state;

    public FormatState(int numOfSubTask, Object state) {
        this.numOfSubTask = numOfSubTask;
        this.state = state;
    }

    public int getNumOfSubTask() {
        return numOfSubTask;
    }

    public void setNumOfSubTask(int numOfSubTask) {
        this.numOfSubTask = numOfSubTask;
    }

    public Object getState() {
        return state;
    }

    public void setState(Object state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "FormatState{" +
                "numOfSubTask=" + numOfSubTask +
                ", state=" + state +
                '}';
    }
}
