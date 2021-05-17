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

package com.dtstack.flinkx.solr.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.solr.SolrConstant;
import com.dtstack.flinkx.solr.SolrUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class SolrInputFormat extends BaseRichInputFormat {


    protected String address;

    protected List<String> filters;

    protected List<MetaColumn> metaColumns;

    protected int batchSize;

    protected List<String> columnNames;

    protected HttpSolrClient client;

    protected SolrQuery solrQuery;

    protected Long startRows;

    protected Long queryRows;

    private Iterator<SolrDocument> iterator;


    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        GenericInputSplit genericInputSplit = (GenericInputSplit) inputSplit;
        client = new HttpSolrClient.Builder(address).build();
        solrQuery = new SolrQuery();
        solrQuery.setQuery(SolrConstant.QUERY_ALL);
        solrQuery.setStart(0);
        solrQuery.setRows(1);
        solrQuery.setFields(columnNames.toArray(new String[columnNames.size()]));
        if (CollectionUtils.isNotEmpty(filters)) {
            solrQuery.setFilterQueries(filters.toArray(new String[filters.size()]));
        }
        try {
            QueryResponse response = client.query(solrQuery);
            SolrDocumentList solrDocumentList = response.getResults();
            long numFound = solrDocumentList.getNumFound();
            queryRows = numFound / genericInputSplit.getTotalNumberOfSplits();
            startRows = queryRows * genericInputSplit.getSplitNumber();
            if (genericInputSplit.getTotalNumberOfSplits() == genericInputSplit.getSplitNumber() + 1) {
                queryRows += numFound % genericInputSplit.getTotalNumberOfSplits();
            }
        } catch (SolrServerException e) {
            throw new IOException("create splits error", e);
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNum) {
        InputSplit[] splits = new InputSplit[splitNum];
        for (int i = 0; i < splitNum; i++) {
            splits[i] = new GenericInputSplit(i, splitNum);
        }
        return splits;
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        return SolrUtils.convertDocumentToRow(iterator.next(), metaColumns);
    }

    @Override
    protected void closeInternal() throws IOException {
        if (client != null) {
            client.close();
            client = null;
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
        if (startRows >= queryRows) {
            return true;
        }
        Long thisBatchSize = Math.min(batchSize, queryRows - startRows);

        solrQuery.setStart(Integer.valueOf(startRows.toString()));
        solrQuery.setRows(Integer.valueOf(thisBatchSize.toString()));
        SolrDocumentList solrDocumentList;
        try {
            QueryResponse response = client.query(solrQuery);
            solrDocumentList = response.getResults();
        } catch (SolrServerException e) {
            LOG.error("get batch data error,startRows is {},batchSize is {}", startRows, thisBatchSize);
            throw new IOException(e);
        }
        if (CollectionUtils.isEmpty(solrDocumentList)) {
            return true;
        }
        this.iterator = solrDocumentList.iterator();
        startRows += thisBatchSize;
        return false;

    }


}
