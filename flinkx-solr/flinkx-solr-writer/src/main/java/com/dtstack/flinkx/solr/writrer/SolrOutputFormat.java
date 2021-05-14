package com.dtstack.flinkx.solr.writrer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.types.Row;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SolrOutputFormat extends BaseRichOutputFormat {

    protected String address;

    protected List<MetaColumn> metaColumns;

    protected HttpSolrClient client;



    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        client = new HttpSolrClient.Builder(address).build();
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        SolrInputDocument solrDocument = new SolrInputDocument();
        int columnIndex = 0;
        for (MetaColumn metaColumn : metaColumns) {
            solrDocument.setField(metaColumn.getName(), row.getField(columnIndex));
            columnIndex++;
        }
        try {
            client.add(solrDocument);
            client.commit();
        } catch (Exception e) {
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        Collection solrInputDocuments = new ArrayList<SolrInputDocument>();
        for (Row row : rows) {
            SolrInputDocument solrDocument = new SolrInputDocument();
            int columnIndex = 0;
            for (MetaColumn metaColumn : metaColumns) {
                solrDocument.setField(metaColumn.getName(), row.getField(columnIndex));
                columnIndex++;
            }
            solrInputDocuments.add(solrDocument);
        }
        client.add(solrInputDocuments);
        client.commit();
    }


    @Override
    public void closeInternal() throws IOException {
        try {
            if (client != null) {
                client.commit();
                client.close();
                client = null;
            }
        } catch (SolrServerException e) {
            throw new IOException("commit client error", e);
        }
    }
}
