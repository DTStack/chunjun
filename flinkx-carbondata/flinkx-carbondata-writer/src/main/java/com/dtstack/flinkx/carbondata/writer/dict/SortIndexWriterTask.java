package com.dtstack.flinkx.carbondata.writer.dict;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.service.CarbonCommonFactory;
import org.apache.carbondata.core.service.DictionaryService;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;

import java.io.IOException;
import java.util.List;

/**
 * This task writes sort index file
*/
public class SortIndexWriterTask {

    private DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier;

    private DataType dataType;

    private Dictionary dictionary;

    private List<String> distinctValues;

    private CarbonDictionarySortIndexWriter carbonDictionarySortIndexWriter;

    public SortIndexWriterTask(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, DataType dataType, Dictionary dictionary, List<String> distinctValues) {
        this.dictionaryColumnUniqueIdentifier = dictionaryColumnUniqueIdentifier;
        this.dataType = dataType;
        this.dictionary = dictionary;
        this.distinctValues = distinctValues;
    }

    public void execute() throws IOException {
        try {
            if(distinctValues.size() > 0) {
                CarbonDictionarySortInfoPreparator preparator = new CarbonDictionarySortInfoPreparator();
                DictionaryService dictService = CarbonCommonFactory.getDictionaryService();
                CarbonDictionarySortInfo dictionarySortInfo = preparator.getDictionarySortInfo(distinctValues, dictionary, dataType);
                carbonDictionarySortIndexWriter = dictService.getDictionarySortIndexWriter(dictionaryColumnUniqueIdentifier);
                carbonDictionarySortIndexWriter.writeSortIndex(dictionarySortInfo.getSortIndex());
                carbonDictionarySortIndexWriter.writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted());
            }
        } finally{
            if (null != carbonDictionarySortIndexWriter) {
                carbonDictionarySortIndexWriter.close();
            }
        }
    }

}
