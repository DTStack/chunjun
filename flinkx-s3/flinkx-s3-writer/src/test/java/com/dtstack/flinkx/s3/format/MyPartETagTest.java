package com.dtstack.flinkx.s3.format;

import com.amazonaws.services.s3.model.PartETag;
import com.dtstack.flinkx.config.SpeedConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * company www.dtstack.com
 * @author jier
 */
public class MyPartETagTest implements Serializable {

    @Test
    public void testMyPartTag(){
        PartETag partETag = mock(PartETag.class);
        when(partETag.getPartNumber()).thenReturn(1);
        when(partETag.getETag()).thenReturn("test");
        MyPartETag myPartETag = new MyPartETag(partETag);
        PartETag newPartETag = myPartETag.genPartETag();

        Assert.assertEquals(partETag.getPartNumber(),newPartETag.getPartNumber());
        Assert.assertEquals(partETag.getETag(),newPartETag.getETag());
    }
}
