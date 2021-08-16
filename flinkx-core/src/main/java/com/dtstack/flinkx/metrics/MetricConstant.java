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

package com.dtstack.flinkx.metrics;

/**
 * defined customer metric name Date: 2018/10/18 Company: www.dtstack.com
 *
 * @author xuchao
 */
public class MetricConstant {

    /** metric name of dirty data */
    public static final String DT_DIRTY_DATA_COUNTER = "dtDirtyData";

    public static final String DT_NUM_RECORDS_IN_COUNTER = "dtNumRecordsIn";

    public static final String DT_NUM_RECORDS_IN_RATE = "dtNumRecordsInRate";

    public static final String DT_NUM_BYTES_IN_COUNTER = "dtNumBytesIn";

    public static final String DT_NUM_BYTES_IN_RATE = "dtNumBytesInRate";

    /** diff of DT_NUM_RECORD_IN_COUNTER ,this metric is desc record num after of deserialization */
    public static final String DT_NUM_RECORDS_RESOVED_IN_COUNTER = "dtNumRecordsInResolve";

    public static final String DT_NUM_RECORDS_RESOVED_IN_RATE = "dtNumRecordsInResolveRate";

    public static final String DT_NUM_RECORDS_OUT = "dtNumRecordsOut";

    public static final String DT_NUM_DIRTY_RECORDS_OUT = "dtNumDirtyRecordsOut";

    public static final String DT_NUM_SIDE_PARSE_ERROR_RECORDS = "dtNumSideParseErrorRecords";

    public static final String DT_NUM_RECORDS_OUT_RATE = "dtNumRecordsOutRate";

    public static final String DT_EVENT_DELAY_GAUGE = "dtEventDelay";

    public static final String DT_TOPIC_PARTITION_LAG_GAUGE = "flinkxTopicPartitionLag";

    public static final String DT_TOPIC_GROUP = "topic";

    public static final String DT_PARTITION_GROUP = "partition";
}
