package com.dtstack.flinkx.metadataphoenix5.reader;

import com.dtstack.flinkx.metadata.inputformat.MetadataInputFormatBuilder;
import com.dtstack.flinkx.metadataphoenix5.inputformat.Metadataphoenix5InputFormat;

/**
 * @author kunni@dtstack.com
 */
public class MetadataPhoenixBuilder extends MetadataInputFormatBuilder {

    protected Metadataphoenix5InputFormat format;

    public MetadataPhoenixBuilder(Metadataphoenix5InputFormat format) {
        super(format);
        this.format = format;
    }

    public void setPath(String path){
        format.setPath(path);
    }
}
