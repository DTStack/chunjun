package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import java.util.HashMap;
import java.util.Map;
import java.util.List;


public class DictionaryDetailHelper {

    public static DictionaryDetail getDictionaryDetail(String dictfolderPath, List<CarbonDimension> primDimensions, String tablePath) {
        String[] dictFilePaths = new String[primDimensions.size()];
        Boolean[] dictFileExists  = new Boolean[primDimensions.size()];
        ColumnIdentifier[] columnIdentifier = new ColumnIdentifier[primDimensions.size()];

        FileFactory.FileType fileType = FileFactory.getFileType(dictfolderPath);
        // Metadata folder
        CarbonFile metadataDirectory = FileFactory.getCarbonFile(dictfolderPath, fileType);
        // need list all dictionary file paths with exists flag
        CarbonFile[] carbonFiles = metadataDirectory.listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile pathname) {
                return CarbonTablePath.isDictionaryFile(pathname);
            }
        });
        // 2 put dictionary file names to fileNamesMap
        Map<String, Integer> fileNamesMap = new HashMap<>();
        for(int i = 0; i < carbonFiles.length; ++i) {
            fileNamesMap.put(carbonFiles[i].getName(), i);
        }
        // 3 lookup fileNamesMap, if file name is in fileNamesMap, file is exists, or not.
        for(int i = 0; i < primDimensions.size(); ++i) {
            columnIdentifier[i] = primDimensions.get(i).getColumnIdentifier();
            dictFilePaths[i] = CarbonTablePath.getDictionaryFilePath(tablePath, primDimensions.get(i).getColumnId());
            Integer v = fileNamesMap.get(CarbonTablePath.getDictionaryFileName(primDimensions.get(i).getColumnId()));
            if(v == null) {
                dictFileExists[i] = false;
            } else {
                dictFileExists[i] = true;
            }
        }

        return new DictionaryDetail(columnIdentifier, dictFilePaths, dictFileExists);
    }

}
