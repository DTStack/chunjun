package com.dtstack.flinkx.s3;


import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class ReaderUtilTest {


    private static final String UPLOAD_FILE_NAME = "src/test/resources/people.csv";

    @Test
    public void testReadFile(){
        final File readFile = new File(UPLOAD_FILE_NAME);
        try (FileInputStream fis = new FileInputStream(readFile)){
            ReaderUtil readerUtil = new ReaderUtil(fis,',', StandardCharsets.UTF_8);
            int i = 0;
            while (readerUtil.readRecord()){
                String[] values = readerUtil.getValues();
                System.out.println(String.join(",", values));
                i++;
            }
            System.out.println("total record is "+i);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
