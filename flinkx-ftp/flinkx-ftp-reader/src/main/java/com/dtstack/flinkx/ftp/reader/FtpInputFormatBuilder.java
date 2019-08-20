package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import java.util.List;


public class FtpInputFormatBuilder extends RichInputFormatBuilder {

    private FtpInputFormat format;

    public FtpInputFormatBuilder() {
        super.format = format = new FtpInputFormat();
    }

    public void setPath(String path) {
        if(StringUtils.isEmpty(path)) {
            format.path = "/";
        } else {
            format.path = path;
        }
    }

    public void setHost(String host) {
        format.host = Preconditions.checkNotNull(host);
    }

    public void setPort(int port) {
        format.port = port;
    }

    public void setUsername(String username) {
        format.username = Preconditions.checkNotNull(username);
    }

    public void setPassword(String password) {
        format.password = Preconditions.checkNotNull(password);
    }

    public void setDelimiter(String delimiter) {
        if(StringUtils.isNotEmpty(delimiter)) {
            format.delimiter = delimiter;
        }
    }

    public void setProtocol(String protocol) {
        format.protocol = Preconditions.checkNotNull(protocol);
    }

    public void setConnectMode(String connectMode) {
        if(StringUtils.isNotEmpty(connectMode)) {
            format.connectMode = connectMode;
        }
    }

    public void setEncoding(String encoding) {
        if(StringUtils.isNotEmpty(encoding)) {
            format.charsetName = encoding;
        }
    }

    public void setTimeout(Integer timeout){
        format.timeout = timeout;
    }

    public void setMetaColumn(List<MetaColumn> metaColumns) {
        format.metaColumns = metaColumns;
    }

    public void setIsFirstLineHeader(boolean isFirstLineHeader){
        format.isFirstLineHeader = isFirstLineHeader;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
