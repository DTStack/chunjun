package com.dtstack.flinkx.s3;


import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * company www.dtstack.com
 * @author jier
 */
public class WriterUtilTest {


    private static final String UPLOAD_FILE_NAME = "src/test/resources/people_target.csv";

    @Test
    public void testWriteFile(){

        final File readFile = new File(UPLOAD_FILE_NAME);
        try (FileInputStream fis = new FileInputStream(readFile)){
            ReaderUtil readerUtil = new ReaderUtil(fis,',', StandardCharsets.UTF_8);
            int i = 0;
            while (readerUtil.readRecord()){
                String[] values = readerUtil.getValues();
//                WriterUtil.
            }
            System.out.println("total record is "+i);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
