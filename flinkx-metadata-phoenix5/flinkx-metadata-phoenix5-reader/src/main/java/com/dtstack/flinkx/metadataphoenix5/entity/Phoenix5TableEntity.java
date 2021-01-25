package com.dtstack.flinkx.metadataphoenix5.entity;


import com.dtstack.metadata.rdb.core.entity.TableEntity;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 16:13
 * @Description:
 */
public class Phoenix5TableEntity extends TableEntity {
    private String nameSpace;

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }
}
