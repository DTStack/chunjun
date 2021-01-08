package com.dtstack.flinkx.config;

import java.io.Serializable;
import java.util.List;

/**
 * The configuration of Converter
 *
 * @author liuwenjie
 * @date 2019/12/12 15:41
 */
public class ConvertConfig implements Serializable {

    List convertList;

    public ConvertConfig(List list) {
        convertList=list;
    }

    public List getConvertList() {
        return convertList;
    }
}
