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

import com.google.gson.Gson;
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
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class NexusRepositoryContainer extends GenericContainer<NexusRepositoryContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NexusRepositoryContainer.class);

    private final Gson gson = new Gson();
    private final HttpClient client = HttpClient.newHttpClient();
    private String adminPassword;
    private boolean acceptedEula;

    public NexusRepositoryContainer() {
        super(DockerImageName.parse("sonatype/nexus3:3.89.1"));
        setExposedPorts(List.of(8081));
        setLogConsumers(List.of(outputFrame -> LOGGER.info("{}", outputFrame.getUtf8StringWithoutLineEnding())));
        setStartupCheckStrategy(new NexusRepositoryStartupCheckStrategy().withTimeout(Duration.ofMinutes(1)));
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
                                "proprietaryComponents": false
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

    public String uploadJarGetUrl(String repositoryName, String artifactId, String content) {
        acceptEula();
        sendAndLog(requestWithAuth()
                .uri(URI.create(getBaseUrl() + "/service/rest/v1/components?repository=" + repositoryName))
                .header("Content-Type", "multipart/form-data; boundary=PART")
                .header("Accept", "application/json")
                .POST(BodyPublishers.ofString(getMultipartDataWithBoundary("PART", Map.of(
                        "maven2.groupId", "org.group",
                        "maven2.artifactId", artifactId,
                        "maven2.version", "1.0",
                        "maven2.asset1", new MultipartFile("test.jar", content),
                        "maven2.asset1.extension", "jar"))))
                .build());
        return getBaseUrl() + "/repository/" + repositoryName + "/org/group/" + artifactId + "/1.0/" + artifactId + "-1.0.jar";
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
        return setAuth(HttpRequest.newBuilder());
    }

    public HttpRequest.Builder setAuth(HttpRequest.Builder builder) {
        String auth = "admin:" + getAdminPassword();
        String encoded = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        return builder.header("Authorization", "Basic " + encoded);
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

    private void acceptEula() {
        if (acceptedEula) {
            return;
        }
        EulaStatus eulaStatus = getEulaStatus();
        if (eulaStatus.accepted()) {
            acceptedEula = true;
            return;
        }
        sendAndLog(requestWithAuth()
                .uri(URI.create(getBaseUrl() + "/service/rest/v1/system/eula"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(BodyPublishers.ofString(String.format("{\"accepted\":true,\"disclaimer\":\"%s\"}", eulaStatus.disclaimer())))
                .build());
        acceptedEula = true;
    }

    private EulaStatus getEulaStatus() {
        String json = sendAndLog(requestWithAuth()
                .uri(URI.create(getBaseUrl() + "/service/rest/v1/system/eula"))
                .header("Accept", "application/json")
                .GET()
                .build());
        return gson.fromJson(json, EulaStatus.class);
    }

    private record EulaStatus(boolean accepted, String disclaimer) {
    }

    private String sendAndLog(HttpRequest request) {
        return sendAndLog(request, BodyHandlers.ofString());
    }

    private <T> T sendAndLog(HttpRequest request, BodyHandler<T> bodyHandler) {
        try {
            HttpResponse<T> response = client.send(request, bodyHandler);
            LOGGER.info("Response: {}", response);
            LOGGER.info("Body: {}", response.body());
            if (response.statusCode() < 200 || response.statusCode() > 299) {
                throw new RuntimeException("Request failed with status " + response.statusCode());
            }
            return response.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String getMultipartDataWithBoundary(String boundary, Map<String, Object> formData) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : formData.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            builder.append("--").append(boundary).append("\r\n");
            if (value instanceof String string) {
                builder.append("Content-Disposition: form-data; name=\"").append(name).append("\"\r\n");
                builder.append("\r\n");
                builder.append(string);
            } else if (value instanceof MultipartFile file) {
                builder.append("Content-Disposition: form-data; name=\"").append(name).append("\"; ")
                        .append("filename=\"").append(file.filename()).append("\"\r\n");
                builder.append("Content-Type: text/plain\r\n");
                builder.append("\r\n");
                builder.append(file.content());
            } else {
                throw new IllegalArgumentException("Unsupported value type: " + value);
            }
            builder.append("\r\n");
        }
        builder.append("--").append(boundary);
        LOGGER.info("Generated multipart data: {}", builder);
        return builder.toString();
    }

    private record MultipartFile(String filename, String content) {
    }

}
