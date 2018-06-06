package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.mongodb.MongodbUtil;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.writer.WriteMode;
import com.mongodb.client.MongoCollection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.mongodb.MongodbConfigKeys.*;

/**
 * @author jiangbo
 * @date 2018/6/5 21:17
 */
public class MongodbOutputFormat extends RichOutputFormat {

    protected String hostPorts;

    protected String username;

    protected String password;

    protected String database;

    protected String collectionName;

    protected List<String> columnNames;

    protected List<String> columnTypes;

    protected List<String> filterColumns;

    protected List<String> updateColumns;

    protected String mode = WriteMode.INSERT.getMode();

    private MongoCollection<Document> collection;

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        Map<String,String> config = new HashMap<>(4);
        config.put(KEY_HOST_PORTS,hostPorts);
        config.put(KEY_USERNAME,username);
        config.put(KEY_PASSWORD,password);
        config.put(KEY_DATABASE,database);

        collection = MongodbUtil.getCollection(config,database,collectionName);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        Document doc = MongodbUtil.convertRowToDoc(row,columnNames,updateColumns);
        Document filter = getFilter(row);

        if(WriteMode.INSERT.getMode().equals(mode)){
            collection.insertOne(doc);
        } else if(WriteMode.UPDATE.getMode().equals(mode)) {
            doc = new Document("$set",doc);
            collection.updateOne(filter,doc);
        } else if(WriteMode.REPLACE.getMode().equals(mode)){
            collection.replaceOne(filter,doc);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        List<Document> documents = new ArrayList<>(rows.size());
        for (Row row : rows) {
            documents.add(MongodbUtil.convertRowToDoc(row,columnNames,null));
        }

        if(WriteMode.INSERT.getMode().equals(mode)){
            collection.insertMany(documents);
        } else if(WriteMode.UPDATE.getMode().equals(mode)) {
            throw new RuntimeException("Does not support batch update documents");
        } else if(WriteMode.REPLACE.getMode().equals(mode)){
            throw new RuntimeException("Does not support batch replace documents");
        }
    }

    @Override
    public void closeInternal() throws IOException {
        super.closeInternal();
        MongodbUtil.close();
    }

    private Document getFilter(Row row){
        Document filter = new Document();
        for (int i = 0; i < filterColumns.size(); i++) {
            filter.put(filterColumns.get(i),row.getField(i));
        }
        return filter;
    }
}
