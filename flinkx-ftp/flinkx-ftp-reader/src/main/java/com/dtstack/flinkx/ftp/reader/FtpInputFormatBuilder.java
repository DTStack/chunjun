package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.ftp.FtpConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author jiangbo
 */
public class FtpInputFormatBuilder extends BaseRichInputFormatBuilder {

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

        if (StringUtils.isEmpty(format.ftpConfig.getPath())) {
            throw new IllegalArgumentException("The property [path] cannot be empty or null");
        }
    }
}
