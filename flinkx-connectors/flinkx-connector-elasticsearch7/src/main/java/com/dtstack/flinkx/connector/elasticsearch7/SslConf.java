package com.dtstack.flinkx.connector.elasticsearch7;

import java.io.Serializable;
import java.util.Map;

public class SslConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** whether to use local files */
    private boolean useLocalFile;

    /** SSL file name * */
    private String fileName;
    /** SSL Folder Path * */
    private String filePath;
    /** Ssl file Password * */
    private String keyStorePass = "";
    /** ssl type pkcs12 or ca * */
    private String type = "pkcs12";
    /** sftp config * */
    private Map<String, Object> sftpConf;

    public boolean isUseLocalFile() {
        return useLocalFile;
    }

    public void setUseLocalFile(boolean useLocalFile) {
        this.useLocalFile = useLocalFile;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getKeyStorePass() {
        return keyStorePass;
    }

    public void setKeyStorePass(String keyStorePass) {
        this.keyStorePass = keyStorePass;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getSftpConf() {
        return sftpConf;
    }

    public void setSftpConf(Map<String, Object> sftpConf) {
        this.sftpConf = sftpConf;
    }
}
