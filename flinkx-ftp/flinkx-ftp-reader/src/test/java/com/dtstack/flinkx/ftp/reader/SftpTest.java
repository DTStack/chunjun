package com.dtstack.flinkx.ftp.reader;

import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.ftp.SFtpHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by softfly on 17/11/23.
 */
public class SftpTest {
    public static void main(String[] args) throws IOException {

        String host = "node02";
        int port = 22;
        String username = "mysftp";
        String password = "oh1986mygod";

        IFtpHandler ftpHandler = new SFtpHandler();
        ftpHandler.loginFtpServer(host,username,password, FtpConfigConstants.DEFAULT_SFTP_PORT,
                0,
                FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN);

        List<String> list = ftpHandler.getFiles("/");

        InputStream is = new FtpSeqInputStream(ftpHandler, list);
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String line = br.readLine();
        while(line != null) {
            System.out.println(line);
            line = br.readLine();
        }

        ftpHandler.logoutFtpServer();


    }
}
