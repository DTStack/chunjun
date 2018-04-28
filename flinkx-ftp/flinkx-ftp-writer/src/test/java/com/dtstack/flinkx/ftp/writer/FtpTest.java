package com.dtstack.flinkx.ftp.writer;

import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.ftp.FtpHandler;
import com.dtstack.flinkx.ftp.SFtpHandler;
import com.dtstack.flinkx.ftp.StandardFtpHandler;

import java.io.IOException;
import java.io.OutputStream;

public class FtpTest {

    public static FtpHandler getStandardFtpUtil() {
        String host = "node02";
        Integer port = 21;
        String username = "test";
        String password = "qbI#5pNd";
        FtpHandler ftpHandler = new StandardFtpHandler();
        ftpHandler.loginFtpServer(host, username, password, port, 60000, FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN);
        return ftpHandler;
    }

    public static FtpHandler getSftpUtil() {
        String host = "node02";
        Integer port = 22;
        String username = "mysftp";
        String password = "oh1986mygod";
        FtpHandler ftpHandler = new SFtpHandler();
        ftpHandler.loginFtpServer(host, username, password, port, 60000, FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN);
        return ftpHandler;
    }

    public static void standardFtpMkDir() {
        FtpHandler ftpHandler = getStandardFtpUtil();
        ftpHandler.mkDirRecursive("/hallo/nani/xxxx");
        ftpHandler.logoutFtpServer();
    }

    public static void sftpMkDir() {
        FtpHandler ftpHandler = getSftpUtil();
        ftpHandler.mkDirRecursive("/uuu");
        ftpHandler.logoutFtpServer();
    }

    public static void standardFtpDeleteFilesInDir() {
        FtpHandler ftpHandler = getStandardFtpUtil();
        ftpHandler.deleteAllFilesInDir("/hallo/hehe");
        ftpHandler.logoutFtpServer();
    }

    public static void sftpDeleteFilesInDir() {
        FtpHandler ftpHandler = getSftpUtil();
        ftpHandler.deleteAllFilesInDir("/upload/hallo");
        ftpHandler.logoutFtpServer();
    }

    public static void streamTest() throws IOException {
        FtpHandler ftpHandler = getStandardFtpUtil();
        ftpHandler.mkDirRecursive("/h1/h2/h3/h4");
        OutputStream os = ftpHandler.getOutputStream("//h1/h2/h3/h4/uuu.txt");
        os.write("ssss".getBytes());
        os.flush();
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        streamTest();
        //standardFtpMkDir();
        //sftpMkDir();
        //standardFtpDeleteFilesInDir();
        //sftpDeleteFilesInDir();
    }

}
