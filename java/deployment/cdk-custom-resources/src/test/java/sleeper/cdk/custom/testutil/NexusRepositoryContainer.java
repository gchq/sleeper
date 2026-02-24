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
package sleeper.cdk.custom.testutil;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class NexusRepositoryContainer extends GenericContainer<NexusRepositoryContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NexusRepositoryContainer.class);

    private String adminPassword;

    public NexusRepositoryContainer() {
        super(DockerImageName.parse("sonatype/nexus3"));
        setExposedPorts(List.of(8081));
        setLogConsumers(List.of(outputFrame -> LOGGER.info("{}", outputFrame.getUtf8StringWithoutLineEnding())));
        setStartupCheckStrategy(new NexusRepositoryStartupCheckStrategy());
    }

    public void checkHealth() {
        sendAndLog(HttpRequest.newBuilder(URI.create(getBaseUrl() + "/")).GET().build());
    }

    public void createRepository(String repositoryName) {
        sendAndLog(requestWithAuth()
                .uri(URI.create(getBaseUrl() + "/service/rest/v1/repositories/maven/hosted"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(BodyPublishers.ofString(String.format("""
                        {
                            "name": "%s",
                            "online": true,
                            "component": {
                                "proprietaryComponents": true
                            },
                            "storage": {
                                "blobStoreName": "default",
                                "strictContentTypeValidation": false,
                                "writePolicy": "allow_once"
                            },
                            "maven": {
                                "versionPolicy": "MIXED",
                                "layoutPolicy": "STRICT",
                                "contentDisposition": "ATTACHMENT"
                            }
                        }
                        """, repositoryName))).build());
    }

    public void listComponents(String repositoryName) {
        sendAndLog(requestWithAuth()
                .uri(URI.create(getBaseUrl() + "/service/rest/v1/components?repository=" + repositoryName))
                .build());
    }

    public String getBaseUrl() {
        return "http://" + getHost() + ":" + getFirstMappedPort();
    }

    private HttpRequest.Builder requestWithAuth() {
        String auth = "admin:" + getAdminPassword();
        String encoded = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        return HttpRequest.newBuilder()
                .header("Authorization", "Basic " + encoded);
    }

    private String getAdminPassword() {
        if (adminPassword == null) {
            adminPassword = copyFileFromContainer("/nexus-data/admin.password", stream -> {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                IOUtils.copy(stream, output);
                return output.toString(StandardCharsets.UTF_8);
            });
        }
        return adminPassword;
    }

    private void sendAndLog(HttpRequest request) {
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, BodyHandlers.ofString());
            LOGGER.info("Response: {}", response);
            LOGGER.info("Body: {}", response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
