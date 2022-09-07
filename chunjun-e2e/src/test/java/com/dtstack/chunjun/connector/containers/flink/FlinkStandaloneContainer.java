/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.containers.flink;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * @author jayce
 * @version 1.0
 * @date 2022/8/11 17:00
 */
public class FlinkStandaloneContainer extends GenericContainer<FlinkStandaloneContainer> {
    private static final URL FLINK_STANDALONE_DOCKERFILE =
            FlinkStandaloneContainer.class
                    .getClassLoader()
                    .getResource("docker/flink/standalone/Dockerfile");

    public FlinkStandaloneContainer(String imageName) throws URISyntaxException {
        super(
                new ImageFromDockerfile(imageName, true)
                        .withDockerfile(Paths.get(FLINK_STANDALONE_DOCKERFILE.toURI())));
        waitingFor(
                new WaitStrategy() {
                    @Override
                    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {}

                    @Override
                    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
                        return null;
                    }
                });
    }
}
