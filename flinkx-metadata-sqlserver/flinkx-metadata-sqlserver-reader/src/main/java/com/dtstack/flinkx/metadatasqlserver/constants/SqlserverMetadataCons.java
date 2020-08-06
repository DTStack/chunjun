/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadatasqlserver.constants;

import com.dtstack.flinkx.metadata.MetaDataCons;

/**
 * @author : kunni@dtstack.com
 * @date : 2020/08/06
 */

public class SqlserverMetadataCons extends MetaDataCons {

    public static final String KEY_DATABASE = "database";

    public static final String SQL_SWITCH_DATABASE = "USE %s";
    //拼接成schema.table
    public static final String SQL_SHOW_TABLES = "SELECT OBJECT_SCHEMA_NAME(object_id, DB_ID()) + '.' + name as name FROM sys.tables";
}
