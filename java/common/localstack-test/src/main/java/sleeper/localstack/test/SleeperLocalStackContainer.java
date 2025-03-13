/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.localstack.test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A helper class to create test containers to run LocalStack.
 */
public class SleeperLocalStackContainer {
    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperLocalStackContainer.class);
    public static final String LOCALSTACK_DOCKER_IMAGE = "localstack/localstack:4.0.3";

    private SleeperLocalStackContainer() {
    }

    public static final LocalStackContainer INSTANCE = start();

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    private static LocalStackContainer start() {
        LocalStackContainer container = new LocalStackContainer(DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE))
                .withServices(Service.S3, Service.DYNAMODB, Service.SQS, Service.STS, Service.CLOUDWATCH)
                .withEnv("LOCALSTACK_HOST", getHostAddress())
                .withLogConsumer(outputFrame -> LOGGER.info(outputFrame.getUtf8StringWithoutLineEnding()))
                .withEnv("DEBUG", "1");
        container.start();
        return container;
    }

    private static String getHostAddress() {
        String dockerHost = DockerClientFactory.instance().dockerHostIpAddress();
        try {
            return InetAddress.getByName(dockerHost).getHostAddress();
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
