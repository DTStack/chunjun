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

package com.dtstack.chunjun.format.excel.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ExcelFormatOptions {

    public static final ConfigOption<Boolean> USE_EXCEL_FORMAT =
            ConfigOptions.key("use-excel-format")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("use excel format");

    public static final ConfigOption<String> SHEET_NO =
            ConfigOptions.key("sheet-no")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sheet no, Multiple numbers separated by commas(,)");
    public static final ConfigOption<String> COLUMN_INDEX =
            ConfigOptions.key("column-index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("column index, Multiple numbers separated by commas(,)");
}
