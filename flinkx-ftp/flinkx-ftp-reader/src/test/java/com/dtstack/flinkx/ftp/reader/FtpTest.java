package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.ftp.FtpHandler;

import java.io.*;
import java.util.*;

public class FtpTest {

    public static void main(String[] args) throws IOException {

        IFtpHandler ftpHandler = new FtpHandler();
        ftpHandler.loginFtpServer("node02",
                "test",
                "qbI#5pNd",
                FtpConfigConstants.DEFAULT_FTP_PORT,
                FtpConfigConstants.DEFAULT_TIMEOUT,
                FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN
                );


        List<String> list = ftpHandler.getFiles("/");
        InputStream is = new FtpSeqInputStream(ftpHandler, list);
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String line = br.readLine();
        while(line != null) {
            System.out.println(line);
            line = br.readLine();
        }

    }

}
