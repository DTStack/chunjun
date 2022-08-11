package com.dtstack.chunjun.connector.test.containers;

import com.dtstack.chunjun.connector.test.ChunjunBaseE2eTest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/8/11 17:00
 */
public class FlinkStandaloneContainer extends GenericContainer<FlinkStandaloneContainer> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkStandaloneContainer.class);

    private static URI FLINK_STANDALONE_DOCKFILE;

    static {
        try {
            FLINK_STANDALONE_DOCKFILE = ChunjunBaseE2eTest.class.getClassLoader().getResource("docker/flink/standalone/Dockerfile").toURI();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    public static final int JOB_MANAGER_REST_PORT = 8081;

    public static final int JOB_MANAGER_RPC_PORT = 6213;


    public FlinkStandaloneContainer(String imageName) {
        super(new ImageFromDockerfile(imageName, false)
                        .withDockerfile(Paths.get(FLINK_STANDALONE_DOCKFILE)));
    }
}
