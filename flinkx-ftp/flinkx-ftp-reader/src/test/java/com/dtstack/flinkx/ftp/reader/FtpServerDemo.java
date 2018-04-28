package com.dtstack.flinkx.ftp.reader;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by softfly on 17/11/22.
 */
public class FtpServerDemo {

    public static void main(String[] args) throws IOException {
        FTPClient ftp = new FTPClient();
        String username = "test";
        String password = "qbI#5pNd";
        ftp.connect("node02", 21);
        ftp.login(username, password);
        FTPFile[] ftpFiles = ftp.listFiles();
        for(FTPFile ftpFile : ftpFiles) {
            System.out.println(ftpFile.getName());
        }


        String[] xxx = ftp.listNames();

        InputStream is1 = ftp.retrieveFileStream("hyf/ttt");
        ftp.getReply();
        InputStream is2 = ftp.retrieveFileStream("xxx");
        ftp.remoteRetrieve("/hyf/ttt");
        ftp.getReply();

        ftp.changeWorkingDirectory("/hyf");
        System.out.println(ftp.printWorkingDirectory());

        ftp.logout();

    }

}
