package com.dtstack.flinkx.carbondata.writer.dict;


import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;


/**
 * Dictionary Writer Task
 */
public class DictionaryWriterTask {

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

    public void execute() {
        String[] values = valuesBuffer.toArray(new String[valuesBuffer.size()]);
        Arrays.sort(values);

    }

    public void updateMetaData() throws IOException {
        if (null != writer) {
            writer.commit();
        }
    }

}
