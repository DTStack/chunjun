package com.dtstack.flinkx.metadatasync.reader;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;

/**
 * @author : tiezhu
 * @date : 2020/3/4
 * @description :
 */
public class MetadatasyncInputFormatBuilderWrong extends JdbcInputFormatBuilder {
    protected MetadatasyncInputFormatWrong format;


    public MetadatasyncInputFormatBuilderWrong(JdbcInputFormat format) {
        super(format);
    }


    @Override
    protected void checkFormat() {
//        if(format.password == null || format.username == null){
//            throw new IllegalArgumentException("缺少连接用户名密码！");
//        }
//        if(format.dbUrl == null){
//            throw new IllegalArgumentException("缺少连接地址！");
//        }
//        if(format.table == null){
//            throw new IllegalArgumentException("缺少表名！");
//        }
    }
}
