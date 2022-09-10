package com.dtstack.chunjun.connector.iceberg.conf;

import com.dtstack.chunjun.conf.ChunJunCommonConf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergWriterConf extends ChunJunCommonConf {
    private String defaultFS;
    private String fileType;
    /** hadoop高可用相关配置 * */
    private Map<String, Object> hadoopConfig = new HashMap<>(16);

    private String path;

    private String writeMode;
    /** upsert模式需要传入主键列表 */
    private List<String> primaryKey = new ArrayList<>();

    public static final String APPEND_WRITE_MODE = "append";
    public static final String UPSERT_WRITE_MODE = "upsert";
    public static final String OVERWRITE_WRITE_MODE = "overwrite";

    public String getDefaultFS() {
        return defaultFS;
    }

    public void setDefaultFS(String defaultFS) {
        this.defaultFS = defaultFS;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public Map<String, Object> getHadoopConfig() {
        return hadoopConfig;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "IcebergWriterConf{" + "path='" + path + '\'' + '}';
    }
}
