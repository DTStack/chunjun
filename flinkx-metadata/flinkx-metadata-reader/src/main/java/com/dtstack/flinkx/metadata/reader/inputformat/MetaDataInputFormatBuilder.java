package com.dtstack.flinkx.metadata.reader.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;

import java.util.List;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 */
public class MetaDataInputFormatBuilder extends RichInputFormatBuilder {
    private MetaDataInputFormat format ;
    public MetaDataInputFormatBuilder(MetaDataInputFormat format){
        super.format = this.format = format;
    }

    public void setDBUrl(String dbUrl) {
        format.dbUrl = dbUrl;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setTable(List<String> table) {
        format.table = table;
    }

    public void setNumPartitions(int numPartitions){
        format.numPartitions = numPartitions;
    }

    public void setDriverName(String driverName){ format.driverName = driverName; }
    @Override
    protected void checkFormat() {
        if (format.password == null || format.username == null) {
            throw new IllegalArgumentException("请检查用户密码是否填写");
        }
        if (format.dbUrl == null) {
            throw new IllegalArgumentException("请检查url是否填写");
        }
    }
}
