package com.dtstack.flinkx.metadataes6.format;


import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;

import java.util.List;
import java.util.Map;

public class Metadataes6InputFormatBuilder extends BaseRichInputFormatBuilder {

    private Metadataes6InputFormat format;

    public Metadataes6InputFormatBuilder() {
        super.format = this.format = new Metadataes6InputFormat();
    }

    public Metadataes6InputFormatBuilder setAddress(String address) {
        format.address = address;
        return this;
    }

    public Metadataes6InputFormatBuilder setUsername(String username) {
        format.username = username;
        return this;
    }

    public Metadataes6InputFormatBuilder setPassword(String password) {
        format.password = password;
        return this;
    }

    public Metadataes6InputFormatBuilder setIndices(List<Object> indices){
        format.indices = indices;
        return this;
    }

    public Metadataes6InputFormatBuilder setClientConfig(Map<String, Object> clientConfig){
        format.clientConfig = clientConfig;
        return this;
    }
    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
