package com.dtstack.flinkx.ftp.writer;

import com.dtstack.flinkx.ftp.FtpConfigConstants;
import com.dtstack.flinkx.ftp.IFtpHandler;
import com.dtstack.flinkx.ftp.SFtpHandler;
import com.dtstack.flinkx.ftp.FtpHandler;

import java.io.IOException;
import java.io.OutputStream;

public class FtpTest {

    public static IFtpHandler getStandardFtpUtil() {
        String host = "node02";
        Integer port = 21;
        String username = "test";
        String password = "qbI#5pNd";
        IFtpHandler ftpHandler = new FtpHandler();
        ftpHandler.loginFtpServer(host, username, password, port, 60000, FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN);
        return ftpHandler;
    }

    public static IFtpHandler getSftpUtil() {
        String host = "node02";
        Integer port = 22;
        String username = "mysftp";
        String password = "oh1986mygod";
        IFtpHandler ftpHandler = new SFtpHandler();
        ftpHandler.loginFtpServer(host, username, password, port, 60000, FtpConfigConstants.DEFAULT_FTP_CONNECT_PATTERN);
        return ftpHandler;
    }

    public static void standardFtpMkDir() {
        IFtpHandler ftpHandler = getStandardFtpUtil();
        ftpHandler.mkDirRecursive("/hallo/nani/xxxx");
        ftpHandler.logoutFtpServer();
    }

    public static void sftpMkDir() {
        IFtpHandler ftpHandler = getSftpUtil();
        ftpHandler.mkDirRecursive("/uuu");
        ftpHandler.logoutFtpServer();
    }

    public static void standardFtpDeleteFilesInDir() {
        IFtpHandler ftpHandler = getStandardFtpUtil();
        ftpHandler.deleteAllFilesInDir("/hallo/hehe", null);
        ftpHandler.logoutFtpServer();
    }

    public static void sftpDeleteFilesInDir() {
        IFtpHandler ftpHandler = getSftpUtil();
        ftpHandler.deleteAllFilesInDir("/upload/hallo", null);
        ftpHandler.logoutFtpServer();
    }

    public static void streamTest() throws IOException {
        IFtpHandler ftpHandler = getStandardFtpUtil();
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
