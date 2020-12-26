package com.dtstack.metadata.rdb.builder;

import com.dtstack.flinkx.metadata.builder.MetadataBaseBuilder;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputFormat;
import com.dtstack.metadata.rdb.inputformat.MetadatardbInputFormat;

/**
 * @author kunni@dtstack.com
 */
public class MetadatardbBuilder extends MetadataBaseBuilder {

    protected MetadatardbInputFormat format;

    public MetadatardbBuilder(MetadatardbInputFormat format){
        super(format);
        this.format = format;
    }

    public void setUsername(String username){
        format.setUsername(username);
    }

    public void setPassword(String password){
        format.setPassword(password);
    }

    public void setUrl(String url){
        format.setUrl(url);
    }

    public void setDriverName(String driverName){
        format.setDriverName(driverName);
    }


}
