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

package com.dtstack.chunjun.connector.starrocks.source.be;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRawTypeMapper;
import com.dtstack.chunjun.connector.starrocks.source.be.entity.ColumnInfo;

import com.starrocks.thrift.TScanBatchResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
public class StarRocksArrowReader {

    private final List<ColumnInfo> columnInfoList;
    private int offsetOfBatchForRead;
    private int rowCountOfBatch;
    private int flinkRowsCount;

    private List<Object[]> sourceJavaRows;
    private final ArrowStreamReader arrowStreamReader;
    private final ConcurrentHashMap<String, FieldVector> fieldVectorMap;
    private final RootAllocator rootAllocator;

    public StarRocksArrowReader(TScanBatchResult nextResult, List<ColumnInfo> columnInfoList) {
        this.columnInfoList = columnInfoList;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        byte[] bytes = nextResult.getRows();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        this.arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, rootAllocator);
        this.offsetOfBatchForRead = 0;
        this.fieldVectorMap = new ConcurrentHashMap<>();
    }

    public StarRocksArrowReader read() throws IOException {
        VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
        while (arrowStreamReader.loadNextBatch()) {
            List<FieldVector> fieldVectors = root.getFieldVectors();
            fieldVectors
                    .parallelStream()
                    .forEach(vector -> fieldVectorMap.put(vector.getName(), vector));
            if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                continue;
            }
            rowCountOfBatch = root.getRowCount();
            sourceJavaRows = new ArrayList<>(rowCountOfBatch);
            for (int i = 0; i < rowCountOfBatch; i++) {
                sourceJavaRows.add(new Object[columnInfoList.size()]);
            }
            this.genData();
            flinkRowsCount += root.getRowCount();
        }
        return this;
    }

    public boolean hasNext() {
        if (offsetOfBatchForRead < flinkRowsCount) {
            return true;
        }
        this.close();
        return false;
    }

    public Object[] next() {
        if (!hasNext()) {
            log.error("offset larger than data count");
            throw new RuntimeException("read offset larger than data count");
        }
        return sourceJavaRows.get(offsetOfBatchForRead++);
    }

    public int getReadRowCount() {
        return flinkRowsCount;
    }

    private void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException e) {
            log.error("Failed to close StarRocksArrowReader:" + e.getMessage());
            throw new RuntimeException("Failed to close StarRocksArrowReader:" + e.getMessage());
        }
    }

    private void genData() {
        columnInfoList
                .parallelStream()
                .forEach(
                        columnInfo -> {
                            FieldVector fieldVector = getFieldVector(columnInfo.getFieldName());
                            getStarRocksToJavaTrans(columnInfo)
                                    .transToJavaData(
                                            fieldVector,
                                            rowCountOfBatch,
                                            columnInfo.getIndex(),
                                            sourceJavaRows);
                        });
    }

    public FieldVector getFieldVector(String fieldName) {
        FieldVector fieldVector = fieldVectorMap.get(fieldName);
        checkNotNull(
                fieldVector, String.format("Can not find StarRocks column data[%s]", fieldName));
        return fieldVector;
    }

    public StarRocksToJavaTrans getStarRocksToJavaTrans(ColumnInfo columnInfo) {
        HashMap<String, StarRocksToJavaTrans> typeTransMap =
                StarRocksToJavaTrans.DataTypeRelationMap.get(columnInfo.getLogicalTypeRoot());
        checkNotNull(typeTransMap, "Unsupported type,columnInfo[%s]", columnInfo);
        StarRocksToJavaTrans starRocksToJavaTrans = typeTransMap.get(columnInfo.getStarRocksType());
        checkNotNull(
                starRocksToJavaTrans,
                "Type corresponding error,Column[%s]'s StarRocksType should be %s;LogicalTypeRoot except %s but is %s",
                columnInfo.getFieldName(),
                columnInfo.getStarRocksType(),
                StarRocksRawTypeMapper.apply(TypeConfig.fromString(columnInfo.getStarRocksType()))
                        .getLogicalType()
                        .getTypeRoot(),
                columnInfo.getLogicalTypeRoot());
        return starRocksToJavaTrans;
    }
}
