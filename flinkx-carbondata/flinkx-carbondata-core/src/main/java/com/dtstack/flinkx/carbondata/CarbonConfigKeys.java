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
package com.dtstack.flinkx.carbondata;

/**
 * This class defines configuration keys for CarbondataReader and CarbondataWriter
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonConfigKeys {

    public static final String KEY_TABLE = "table";

    public static final String KEY_DATABASE = "database";

    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";

    public static final String KEY_TABLE_PATH = "path";

    public static final String KEY_FILTER = "filter";

    public static final String KEY_BATCH_SIZE = "batchSize";

    public static final int DEFAULT_BATCH_SIZE = 200 * 1024;

    public static final String KEY_WRITE_MODE = "writeMode";

    public static final String KEY_DEFAULT_FS = "defaultFS";

    public static final String KEY_PARTITION = "partition";

}
