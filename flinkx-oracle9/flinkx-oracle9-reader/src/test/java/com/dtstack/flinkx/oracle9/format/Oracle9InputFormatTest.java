package com.dtstack.flinkx.oracle9.format;

import oracle.xdb.XMLType;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/11 13:50
 */
public class Oracle9InputFormatTest {


    private static final String xml = "<label>This is an XML fragment</label>";


    @Test
    public void xmlTypeToStringTest() throws Exception {
        Oracle9InputFormat inputFormat = PowerMockito.mock(Oracle9InputFormat.class);
        XMLType xmlType = PowerMockito.mock(XMLType.class);
        PowerMockito.when(xmlType.getCharacterStream()).thenReturn(new InputStreamReader(new ByteArrayInputStream(xml.getBytes())));
        PowerMockito.doCallRealMethod().when(inputFormat).xmlTypeToString(xmlType);
        Assert.assertEquals(xml, inputFormat.xmlTypeToString(xmlType));
    }

}
