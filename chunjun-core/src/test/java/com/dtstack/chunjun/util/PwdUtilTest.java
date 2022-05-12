package com.dtstack.chunjun.util;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

public class PwdUtilTest {
    @Test
    public void testDesensitization() throws IOException {
        String sql = readFile("test_3.sql");
        String desensitization = PwdUtil.desensitization(sql);
        System.out.println(desensitization);
    }

    private String readFile(String fileName) throws IOException {
        // Creating an InputStream object
        try (InputStream inputStream =
                        Objects.requireNonNull(
                                this.getClass().getClassLoader().getResourceAsStream(fileName));
                // creating an InputStreamReader object
                InputStreamReader isReader = new InputStreamReader(inputStream);
                // Creating a BufferedReader object
                BufferedReader reader = new BufferedReader(isReader)) {
            StringBuilder sb = new StringBuilder();
            String str;
            while ((str = reader.readLine()) != null) {
                sb.append(str);
            }

            return sb.toString();
        }
    }
}
