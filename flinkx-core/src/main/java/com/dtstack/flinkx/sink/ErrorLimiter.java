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

package com.dtstack.flinkx.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.metrics.AccumulatorCollector;

/**
 * 脏数据限制器，当脏数据满足以下条件任务，任务报错并停止
 * 1、脏数据记录数超过设置的最大值
 * 2、脏数据比例超过设置的最大值
 */
public class ErrorLimiter {

    private final Integer maxErrors;
    private final Double maxErrorRatio;
    private final AccumulatorCollector accumulatorCollector;
    private volatile double errorRatio = 0.0;
    private String errMsg = "";
    private RowData errorData;

    public ErrorLimiter(AccumulatorCollector accumulatorCollector, Integer maxErrors, Double maxErrorRatio) {
        this.maxErrors = maxErrors;
        this.maxErrorRatio = maxErrorRatio;
        this.accumulatorCollector = accumulatorCollector;
    }

    /**
     * 从累加器收集器中更新脏数据指标信息
     */
    public void updateErrorInfo(){
        accumulatorCollector.collectAccumulator();
    }

    /**
     * 校验脏数据指标是否超过设定值
     */
    public void checkErrorLimit() {
        String errorDataStr = "";
        if(errorData != null){
            errorDataStr = errorData + "\n";
        }

        long errors = accumulatorCollector.getAccumulatorValue(Metrics.NUM_ERRORS, false);
        if(maxErrors != null){
            Preconditions.checkArgument(errors <= maxErrors,
                    "WritingRecordError: error writing record [" + errors + "] exceed limit [" + maxErrors + "]\n" + errorDataStr + errMsg);
        }

        if(maxErrorRatio != null){
            long numRead = accumulatorCollector.getAccumulatorValue(Metrics.NUM_READS, false);
            if(numRead >= 1) {
                errorRatio = (double) errors / numRead;
            }

            Preconditions.checkArgument(errorRatio <= maxErrorRatio,
                    "WritingRecordError: error writing record ratio [" + errorRatio + "] exceed limit [" + maxErrorRatio + "]\n" + errorDataStr + errMsg);
        }
    }

    public void setErrorData(RowData errorData){
        this.errorData = errorData;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }
}
