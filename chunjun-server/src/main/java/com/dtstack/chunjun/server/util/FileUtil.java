package com.dtstack.chunjun.server.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 文件操作相关 Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-07-05
 */
public class FileUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    public static void checkFileExist(String filePath) {
        if (StringUtils.isNotBlank(filePath)) {
            if (!new File(filePath).exists()) {
                throw new RuntimeException(
                        String.format("The file jar %s  path is not exist ", filePath));
            }
        }
    }
}
