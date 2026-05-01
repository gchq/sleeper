/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.lambda.powertools.cloudformation.AbstractCustomResourceHandler;
import software.amazon.lambda.powertools.cloudformation.Response;
import software.amazon.lambda.powertools.cloudformation.Response.Status;

import sleeper.container.images.ContainerImageTransferManager;
import sleeper.container.images.ContainerImageTransferRequest;
import sleeper.container.images.ContainerRegistryCredentials;
import sleeper.container.images.EcrCredentialRetriever;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class CopyContainerImageLambda extends AbstractCustomResourceHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyContainerImageLambda.class);

    private final Client client;

    public CopyContainerImageLambda() {
        this(Client.transferManagerWithSourceCredentials(null));
    }

    public CopyContainerImageLambda(Client client) {
        this.client = client;
    }

    @Override
    protected Response create(CloudFormationCustomResourceEvent event, Context context) {
        return copyImage(event);
    }

    @Override
    protected Response update(CloudFormationCustomResourceEvent event, Context context) {
        return copyImage(event);
    }

    @Override
    protected Response delete(CloudFormationCustomResourceEvent event, Context context) {
        return Response.success(event.getPhysicalResourceId());
    }

    private Response copyImage(CloudFormationCustomResourceEvent event) {
        Map<String, Object> properties = event.getResourceProperties();
        String source = (String) properties.get("source");
        String target = (String) properties.get("target");

        try {
            String digest = client.transferGetDigest(source, target);
            return Response.builder()
                    .status(Status.SUCCESS)
                    .physicalResourceId(target)
                    .value(Map.of("digest", digest))
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            LOGGER.error("Failed to copy container image", e);
            return Response.builder()
                    .status(Status.FAILED)
                    .physicalResourceId(target)
                    .reason("Failed to copy container image: " + e.getMessage())
                    .build();
        }
    }

    public interface Client {
        String transferGetDigest(String source, String target) throws InterruptedException;

        static Client transferManagerWithSourceCredentials(
                ContainerRegistryCredentials.Retriever sourceCredentialRetriever) {
            return transferManager(
                    ContainerImageTransferManager.builder()
                            .allowInsecureRegistries(false)
                            .cacheDir(createTemporaryDirectory())
                            .build(),
                    sourceCredentialRetriever,
                    new EcrCredentialRetriever(EcrClient.create()));
        }

        static Client transferManager(
                ContainerImageTransferManager transferManager,
                ContainerRegistryCredentials.Retriever sourceCredentialRetriever,
                ContainerRegistryCredentials.Retriever targetCredentialRetriever) {
            return (source, target) -> transferManager.transfer(ContainerImageTransferRequest.builder()
                    .sourceImageReference(source)
                    .targetImageReference(target)
                    .sourceCredentialsRetriever(sourceCredentialRetriever)
                    .targetCredentialsRetriever(targetCredentialRetriever)
                    .build()).imageDigest();
        }
    }

    private static Path createTemporaryDirectory() {
        try {
            return Files.createTempDirectory(null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
