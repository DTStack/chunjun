package com.dtstack.chunjun.connector.hbase.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

public class HBaseTestUtil {
    public static String readFile(String fileName) throws IOException {
        // Creating an InputStream object
        try (InputStream inputStream =
                        Objects.requireNonNull(
                                HBaseTestUtil.class
                                        .getClassLoader()
                                        .getResourceAsStream(fileName));
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
