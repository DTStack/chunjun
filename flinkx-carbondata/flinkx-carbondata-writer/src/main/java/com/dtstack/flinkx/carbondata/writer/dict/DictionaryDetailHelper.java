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


import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import java.util.HashMap;
import java.util.Map;
import java.util.List;


/**
 * Dictionary Detail Helper
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
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
        Map<String, Integer> fileNamesMap = new HashMap<>(carbonFiles.length);
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
