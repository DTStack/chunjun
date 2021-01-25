package com.dtstack.flinkx.metadatahive2.inputformat;

import com.dtstack.metadata.rdb.builder.MetadatardbBuilder;

import java.util.Map;

/**
 * Date: 2020/05/26
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Metadatehive2InputFormatBuilder extends MetadatardbBuilder {
    private Metadatahive2InputFormat format;


    public Metadatehive2InputFormatBuilder(Metadatahive2InputFormat format) {
        super(format);
        this.format = format;
    }

    public void setHadoopConfig(Map<String,Object> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

}
