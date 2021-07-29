package com.dtstack.flinkx.ftp;

/**
 * @author jiangbo
 * @date 2020/2/12
 */
public enum EProtocol {

    /**
     * FTP协议
     */
    FTP,

    /**
     * 安全的FTP协议
     */
    SFTP;

    public static EProtocol getByName(String name) {
        for (EProtocol value : EProtocol.values()) {
            if (value.name().equalsIgnoreCase(name)) {
                return value;
            }
        }

        return SFTP;
    }
}
