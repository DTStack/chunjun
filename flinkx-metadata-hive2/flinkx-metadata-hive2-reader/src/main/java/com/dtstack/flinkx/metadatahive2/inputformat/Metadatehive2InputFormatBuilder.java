package com.dtstack.flinkx.metadatahive2.inputformat;

import com.dtstack.flinkx.metadata.inputformat.MetadataInputFormatBuilder;

import java.util.Map;

/**
 * Date: 2020/05/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Metadatehive2InputFormatBuilder extends MetadataInputFormatBuilder {
    private Metadatahive2InputFormat format;


    public Metadatehive2InputFormatBuilder(Metadatahive2InputFormat format) {
        super(format);
        this.format = format;
    }

    public void setHadoopConfig(Map<String,Object> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

}
