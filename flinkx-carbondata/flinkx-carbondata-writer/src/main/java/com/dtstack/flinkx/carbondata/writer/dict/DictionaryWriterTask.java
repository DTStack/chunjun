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


package com.dtstack.flinkx.carbondata.writer.dict;


import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.service.CarbonCommonFactory;
import org.apache.carbondata.core.service.DictionaryService;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;


/**
 * Dictionary Writer Task
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class DictionaryWriterTask {

    private static final Logger LOG = LoggerFactory.getLogger(DictionaryWriterTask.class);

    private Set<String> valuesBuffer;

    private Dictionary dictionary;

    private DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier;

    private ColumnSchema columnSchema;

    private boolean isDictionaryFileExist;

    private CarbonDictionaryWriter writer;

    public DictionaryWriterTask(Set<String> valuesBuffer, Dictionary dictionary, DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, ColumnSchema columnSchema, boolean isDictionaryFileExist) {
        this.valuesBuffer = valuesBuffer;
        this.dictionary = dictionary;
        this.dictionaryColumnUniqueIdentifier = dictionaryColumnUniqueIdentifier;
        this.columnSchema = columnSchema;
        this.isDictionaryFileExist = isDictionaryFileExist;
    }

    public List<String> execute() throws IOException {
        String[] values = valuesBuffer.toArray(new String[valuesBuffer.size()]);
        Arrays.sort(values);
        DictionaryService dictService = CarbonCommonFactory.getDictionaryService();
        writer = dictService.getDictionaryWriter(dictionaryColumnUniqueIdentifier);
        List<String> distinctValues = new ArrayList<>();

        try {
            if (!isDictionaryFileExist) {
                writer.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
                distinctValues.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
            }

            if (values.length >= 1) {
                if (isDictionaryFileExist) {
                    for (String value : values) {
                        String parsedValue = DataTypeUtil.normalizeColumnValueForItsDataType(value, columnSchema);
                        if (null != parsedValue && dictionary.getSurrogateKey(parsedValue) ==
                                CarbonCommonConstants.INVALID_SURROGATE_KEY) {
                            writer.write(parsedValue);
                            distinctValues.add(parsedValue);
                        }
                    }

                } else {
                    for (String value : values) {
                        String parsedValue = DataTypeUtil.normalizeColumnValueForItsDataType(value, columnSchema);
                        if (null != parsedValue) {
                            writer.write(parsedValue);
                            distinctValues.add(parsedValue);
                        }
                    }
                }
            }
        } finally {
            if (null != writer) {
                try {
                    writer.close();
                }catch (IOException e){
                    LOG.error(ExceptionUtil.getErrorMessage(e));
                }
            }
        }
        return distinctValues;
    }

    public void updateMetaData() throws IOException {
        if (null != writer) {
            writer.commit();
        }
    }

}
