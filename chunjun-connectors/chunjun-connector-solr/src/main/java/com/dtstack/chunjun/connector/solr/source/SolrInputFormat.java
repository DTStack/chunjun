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

package com.dtstack.chunjun.connector.solr.source;

import com.dtstack.chunjun.connector.solr.SolrConfig;
import com.dtstack.chunjun.connector.solr.client.CloudSolrClientKerberosWrapper;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.CollectionUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class SolrInputFormat extends BaseRichInputFormat {
    private static final long serialVersionUID = 1959502927016035403L;

    public static final String QUERY_ALL = "*:*";
    private final SolrConfig solrConfig;
    protected String[] fieldNames;
    private CloudSolrClientKerberosWrapper solrClientWrapper;
    private SolrQuery solrQuery;
    private Long startRows;
    private Long maxRows;
    private Iterator<SolrDocument> iterator;

    public SolrInputFormat(SolrConfig solrConfig, String[] fieldNames) {
        this.solrConfig = solrConfig;
        this.fieldNames = fieldNames;
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNum) {
        InputSplit[] splits = new InputSplit[splitNum];
        for (int i = 1; i <= splitNum; i++) {
            splits[i - 1] = new GenericInputSplit(i, splitNum);
        }
        return splits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        solrClientWrapper =
                new CloudSolrClientKerberosWrapper(
                        solrConfig, getRuntimeContext().getDistributedCache());
        solrClientWrapper.init(jobId, String.valueOf(indexOfSubTask));

        GenericInputSplit genericInputSplit = (GenericInputSplit) inputSplit;
        solrQuery = new SolrQuery();
        solrQuery.setQuery(QUERY_ALL);
        solrQuery.setStart(0);
        solrQuery.setRows(0);
        solrQuery.setFields(fieldNames);
        List<String> filterQueries = solrConfig.getFilterQueries();
        if (CollectionUtils.isNotEmpty(filterQueries)) {
            solrQuery.setFilterQueries(filterQueries.toArray(new String[filterQueries.size()]));
        }
        QueryResponse response = solrClientWrapper.query(solrQuery);
        SolrDocumentList solrDocumentList = response.getResults();
        long numFound = solrDocumentList.getNumFound();
        Long queryRows = numFound / genericInputSplit.getTotalNumberOfSplits();
        startRows = queryRows * (genericInputSplit.getSplitNumber() - 1);
        if (genericInputSplit.getTotalNumberOfSplits() == genericInputSplit.getSplitNumber()) {
            queryRows += numFound % genericInputSplit.getTotalNumberOfSplits();
        }
        maxRows = startRows + queryRows;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        try {
            SolrDocument document = iterator.next();
            return rowConverter.toInternal(document);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void closeInternal() {
        if (solrClientWrapper != null) {
            solrClientWrapper.close();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (iterator != null && iterator.hasNext()) {
            return false;
        } else {
            return getNextBatchSize();
        }
    }

    private boolean getNextBatchSize() throws IOException {
        if (startRows >= maxRows) {
            return true;
        }
        Long batchSize = Math.min(solrConfig.getBatchSize(), maxRows - startRows);

        solrQuery.setStart(Integer.valueOf(startRows.toString()));
        solrQuery.setRows(Integer.valueOf(batchSize.toString()));
        SolrDocumentList solrDocumentList;
        QueryResponse response = solrClientWrapper.query(solrQuery);
        solrDocumentList = response.getResults();

        if (CollectionUtils.isEmpty(solrDocumentList)) {
            return true;
        }
        this.iterator = solrDocumentList.iterator();
        startRows += batchSize;
        return false;
    }
}
