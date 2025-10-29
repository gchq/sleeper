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
package sleeper.clients.images;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import sleeper.core.deploy.LambdaHandler;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.Map;

public class LambdaDockerImageTestBase extends DockerImageTestBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(LambdaDockerImageTestBase.class);

    protected void runLambda(String image, LambdaHandler handler, String body) throws Exception {
        try (GenericContainer<?> container = new GenericContainer<>(image)) {

            Map<String, String> environment = getEnvironment();
            environment.put("AWS_ACCESS_KEY_ID", localStackContainer.getAccessKey());
            environment.put("AWS_SECRET_ACCESS_KEY", localStackContainer.getSecretKey());
            environment.put("LOG_LEVEL", "trace");

            LOGGER.info("Running handler {} with image {}", handler.getHandler(), image);
            LOGGER.info("Setting environment: {}", environment);

            container.withEnv(environment)
                    .withCommand(handler.getHandler())
                    .withLogConsumer(outputFrame -> LOGGER.info(outputFrame.getUtf8StringWithoutLineEnding()));
            container.setPortBindings(List.of("9000:8080"));
            container.start();

            HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://" + container.getHost() + ":9000/2015-03-31/functions/function/invocations"))
                            .POST(BodyPublishers.ofString(body))
                            .build(),
                    BodyHandlers.discarding());
        }
    }

}
