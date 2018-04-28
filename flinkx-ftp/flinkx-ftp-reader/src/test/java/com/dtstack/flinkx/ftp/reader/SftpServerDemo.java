package com.dtstack.flinkx.ftp.reader;

import com.jcraft.jsch.*;

import java.util.Properties;
import java.util.Vector;

/**
 * Created by softfly on 17/11/23.
 */
public class SftpServerDemo {

    public static void main(String[] args) throws JSchException, SftpException {
        JSch jsch = new JSch();

        Session session = jsch.getSession("mysftp", "node02");
        session.setPassword("oh1986mygod");
        session.setPort(22);
        //session.setTimeout(10);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");

        session.setConfig(config);
        session.connect();

        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
        channelSftp.connect(); // 建立SFTP通道的连接

        Vector vector =  channelSftp.ls("/");

        for(int i = 0; i < vector.size(); ++i) {
            ChannelSftp.LsEntry le = (ChannelSftp.LsEntry) vector.get(i);
            System.out.println(le.getFilename() );
            System.out.println(le.getLongname());
        }


        //session.disconnect();

    }

}
