package com.dtstack.flinkx.metadatahive2.inputformat;

import com.dtstack.flinkx.metadata.inputformat.MetadataInputFormatBuilder;
import com.dtstack.flinkx.metadatahive2.constants.Hive2Version;

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

    public void setHive2Server(String source, String version){
        format.server = new Hive2Version(source, version);
    }
}
