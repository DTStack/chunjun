package com.dtstack.flinkx.s3.format;

import com.amazonaws.services.s3.AmazonS3;
import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.s3.ReaderUtil;
import com.dtstack.flinkx.s3.format.S3InputFormat;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3InputFormatTest {

    S3InputFormat s3InputFormat;
    private AmazonS3 amazonS3;
    private ReaderUtil readerUtil;
    private RestoreConfig restoreConfig;

    @Before
    public void setup(){
        s3InputFormat = new S3InputFormat();
        amazonS3 = mock(AmazonS3.class);
        readerUtil = mock(ReaderUtil.class);
        restoreConfig = mock(RestoreConfig.class);
    }




    @Test
    public void nextRecordInternalTest() throws Exception {
        List<Map<String,Object>> metaColumns = new ArrayList<>();
        Map<String,Object> map1 = new HashMap<>();
        map1.put("index",0);
        map1.put("type","int");
        Map<String,Object> map2 = new HashMap<>();
        map2.put("index",1);
        map2.put("type","string");
        Map<String,Object> map3 = new HashMap<>();
        map3.put("index",2);
        map3.put("type","double");
        Map<String,Object> map4 = new HashMap<>();
        map4.put("index",3);
        map4.put("type","datetime");
        map4.put("format","YYYY/MM/dd hh:mm:ss");
        metaColumns.add(map1);
        metaColumns.add(map2);
        metaColumns.add(map3);
        metaColumns.add(map4);
        when(restoreConfig.isRestore()).thenReturn(false);
        when(readerUtil.getValues()).thenReturn(new String[]{"1","hello","2.333","2021/04/01 12:01:03"});
        MemberModifier.field(S3InputFormat.class, "metaColumns")
                .set(s3InputFormat, MetaColumn.getMetaColumns(metaColumns,false));
        MemberModifier.field(S3InputFormat.class, "readerUtil")
                .set(s3InputFormat, readerUtil);
        MemberModifier.field(S3InputFormat.class, "restoreConfig")
                .set(s3InputFormat, restoreConfig);
        Row row = new Row(4);
        row.setField(0,1);
        row.setField(1,"hello");
        row.setField(2,2.333d);
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd hh:mm:ss");

        row.setField(3,DateUtil.columnToTimestamp("2021/04/01 12:01:03",sdf));
        Row newRow = s3InputFormat.nextRecordInternal(new Row(1));

        Assert.assertTrue(row.equals(newRow));
    }

    @Test
    public void closeInternalTest() throws Exception {
        Whitebox.setInternalState(s3InputFormat, "readerUtil", readerUtil);
        Whitebox.setInternalState(s3InputFormat, "amazonS3", amazonS3);
        s3InputFormat.closeInternal();
        Assert.assertNull(Whitebox.getInternalState(s3InputFormat,"readerUtil"));
        Assert.assertNull(Whitebox.getInternalState(s3InputFormat,"amazonS3"));
    }
}
