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

package com.dtstack.chunjun.connector.solr.sink;

import com.dtstack.chunjun.connector.solr.SolrConfig;
import com.dtstack.chunjun.connector.solr.client.CloudSolrClientKerberosWrapper;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;

public class SolrOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = 8630384526889401303L;
    private final SolrConfig solrConfig;
    private CloudSolrClientKerberosWrapper solrClientWrapper;

    public SolrOutputFormat(SolrConfig solrConfig) {
        this.solrConfig = solrConfig;
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        SolrInputDocument solrDocument = new SolrInputDocument();
        try {
            rowConverter.toExternal(rowData, solrDocument);
            solrClientWrapper.add(solrDocument);
            solrClientWrapper.commit();
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws WriteRecordException {
        try {
            List<SolrInputDocument> solrInputDocuments = new ArrayList<>();
            for (RowData rowData : rows) {
                SolrInputDocument solrDocument = new SolrInputDocument();
                rowConverter.toExternal(rowData, solrDocument);
                solrInputDocuments.add(solrDocument);
            }
            solrClientWrapper.add(solrInputDocuments);
            solrClientWrapper.commit();
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, rows);
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        solrClientWrapper =
                new CloudSolrClientKerberosWrapper(
                        solrConfig, getRuntimeContext().getDistributedCache());
        solrClientWrapper.init(jobId, String.valueOf(taskNumber));
    }

    @Override
    protected void closeInternal() {
        if (solrClientWrapper != null) {
            solrClientWrapper.close();
        }
    }
}
