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

package com.dtstack.flinkx.s3.format;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseFileOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.s3.ReaderUtil;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.s3.S3Util;
import com.dtstack.flinkx.s3.WriterUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.support.membermodification.MemberModifier;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3OutputFormatTest {

    private S3OutputFormat s3OutputFormat;
    private WriterUtil writerUtil;
    private StringWriter sw ;

    private RestoreConfig restoreConfig;

    @Before
    public void setup(){
        s3OutputFormat = new S3OutputFormat();
        sw = new StringWriter();
        this.writerUtil = new WriterUtil(sw, ',');
        restoreConfig = mock(RestoreConfig.class);
    }

    @Test
    public void testWriteSingleRecordToFile() throws WriteRecordException, IllegalAccessException {
        List<String> columnTypes = new ArrayList<>();
        columnTypes.add("int");
        columnTypes.add("string");
        columnTypes.add("string");
        columnTypes.add("datetime");


        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/YYYY hh:mm:ss");

        Row row = new Row(4);
        row.setField(0,1);
        row.setField(1,"alice");
        row.setField(2,"bafbc2fa-a1d9-11eb-be07-073a10a2fb04");
        row.setField(3,DateUtil.columnToTimestamp("01/04/2021 12:01:03",sdf));

        MemberModifier.field(S3OutputFormat.class, "writerUtil")
                .set(s3OutputFormat, writerUtil);
        MemberModifier.field(S3OutputFormat.class, "columnTypes")
                .set(s3OutputFormat, columnTypes);

        MemberModifier.field(S3OutputFormat.class, "restoreConfig")
                .set(s3OutputFormat, restoreConfig);

        when(restoreConfig.isRestore()).thenReturn(false);
        s3OutputFormat.writeSingleRecordToFile(row);

        Assert.assertEquals("1,alice,bafbc2fa-a1d9-11eb-be07-073a10a2fb04,2020-12-27 00:01:03\n",sw.getBuffer().toString());
    }
}
