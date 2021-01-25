package com.dtstack.flinkx.metastore.builder;


import com.dtstack.flinkx.metadata.builder.MetadataBaseBuilder;
import com.dtstack.flinkx.metastore.inputformat.MetaStoreInputFormat;

import java.util.Map;

/**
 * Date: 2020/05/26
 * Company: www.dtstack.com
 *
 * @author shifang
 */
public class MetaStoreInputFormatBuilder extends MetadataBaseBuilder {
    private MetaStoreInputFormat format;


    public MetaStoreInputFormatBuilder(MetaStoreInputFormat format) {
        super(format);
        this.format = format;
    }

    public void setHadoopConfig(Map<String,Object> hadoopConfig) {
        format.setHadoopConfig(hadoopConfig);
    }

}
