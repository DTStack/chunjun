package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Preconditions;
import java.util.List;


public class FtpInputFormatBuilder extends RichInputFormatBuilder {

    private FtpInputFormat format;

    public FtpInputFormatBuilder() {
        super.format = format = new FtpInputFormat();
    }

    public void setFtpConfig(FtpConfig ftpConfig){
        format.ftpConfig = ftpConfig;
    }

    public void setMetaColumn(List<MetaColumn> metaColumns) {
        format.metaColumns = metaColumns;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}
