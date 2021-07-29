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

package com.dtstack.flinkx.constants;

/**
 * Statistics Metrics
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Metrics {

    public static final String NUM_ERRORS = "nErrors";

    public static final String NUM_NULL_ERRORS = "nullErrors";

    public static final String NUM_DUPLICATE_ERRORS = "duplicateErrors";

    public static final String NUM_CONVERSION_ERRORS = "conversionErrors";

    public static final String NUM_OTHER_ERRORS = "otherErrors";

    public static final String READ_BYTES = "byteRead";

    public static final String READ_DURATION = "readDuration";

    public static final String WRITE_BYTES = "byteWrite";

    public static final String WRITE_DURATION = "writeDuration";

    public static final String NUM_WRITES = "numWrite";

    public static final String SNAPSHOT_WRITES = "snapshotWrite";

    public static final String JOB_NAME = "<job_name>";

    public static final String JOB_ID = "<job_id>";

    public static final String SUBTASK_INDEX = "<subtask_index>";

    public static final String NUM_READS = "numRead";

    public static final String END_LOCATION = "endLocation";

    public static final String START_LOCATION = "startLocation";

    public static final String TABLE_COL = "tableCol";

    public static final String MAX_VALUE = "maxValue";

    public static final String METRIC_GROUP_KEY_FLINKX = "flinkx";

    public static final String METRIC_GROUP_VALUE_OUTPUT = "output";

    public static final String LAST_WRITE_LOCATION_PREFIX = "last_write_location";

    public static final String LAST_WRITE_NUM__PREFIX = "last_write_num";

    public static final String SUFFIX_RATE = "PerSecond";

}
