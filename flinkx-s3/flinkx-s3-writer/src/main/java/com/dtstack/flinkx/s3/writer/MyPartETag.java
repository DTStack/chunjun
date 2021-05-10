package com.dtstack.flinkx.s3.writer;

import com.amazonaws.services.s3.model.PartETag;

import java.io.Serializable;

/**
 * company www.dtstack.com
 * @author jier
 */
public class MyPartETag implements Serializable {
    private static final long serialVersionUID = 1L;

    private int partNumber;

    private String eTag;

    public MyPartETag() {
    }

    public MyPartETag(PartETag partETag) {
        this.partNumber = partETag.getPartNumber();
        this.eTag = partETag.getETag();
    }

    public PartETag genPartETag() {
        return new PartETag(this.partNumber,this.eTag);
    }

    public int getPartNumber() {
        return partNumber;
    }

    public void setPartNumber(int partNumber) {
        this.partNumber = partNumber;
    }

    public String geteTag() {
        return eTag;
    }

    public void seteTag(String eTag) {
        this.eTag = eTag;
    }

    @Override
    public String toString() {
        return partNumber+"$"+eTag;
    }
}
