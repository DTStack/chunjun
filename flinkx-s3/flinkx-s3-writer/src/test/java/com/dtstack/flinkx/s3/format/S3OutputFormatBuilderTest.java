package com.dtstack.flinkx.s3.format;

import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.s3.format.S3OutputFormatBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3OutputFormatBuilderTest {

    private S3OutputFormatBuilder s3OutputFormatBuilder;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup(){
        s3OutputFormatBuilder = new S3OutputFormatBuilder();
    }

    @Test
    public void testCheckFormat(){
        SpeedConfig speedConfig = mock(SpeedConfig.class);
        when(speedConfig.getChannel()).thenReturn(3);
        s3OutputFormatBuilder.setSpeedConfig(speedConfig);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("S3Writer can not support channel bigger than 1, current channel is [3]");
        s3OutputFormatBuilder.checkFormat();
    }
}
