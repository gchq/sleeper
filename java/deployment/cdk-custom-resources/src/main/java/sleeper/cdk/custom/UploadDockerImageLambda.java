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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.google.cloud.tools.jib.api.CacheDirectoryCreationException;
import com.google.cloud.tools.jib.api.Containerizer;
import com.google.cloud.tools.jib.api.CredentialRetriever;
import com.google.cloud.tools.jib.api.InvalidImageReferenceException;
import com.google.cloud.tools.jib.api.Jib;
import com.google.cloud.tools.jib.api.JibContainer;
import com.google.cloud.tools.jib.api.RegistryException;
import com.google.cloud.tools.jib.api.RegistryImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;

import sleeper.cdk.custom.containers.EcrCredentialRetriever;
import sleeper.cdk.custom.containers.JibEvents;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class UploadDockerImageLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadDockerImageLambda.class);

    private final boolean allowInsecureRegistries;
    private final CredentialRetriever sourceCredentialRetriever;
    private final CredentialRetriever targetCredentialRetriever;

    public UploadDockerImageLambda() {
        this(builder().targetCredentialRetriever(new EcrCredentialRetriever(EcrClient.create())));
    }

    protected UploadDockerImageLambda(Builder builder) {
        allowInsecureRegistries = builder.allowInsecureRegistries;
        sourceCredentialRetriever = builder.sourceCredentialRetriever;
        targetCredentialRetriever = builder.targetCredentialRetriever;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, Object> handleEvent(
            CloudFormationCustomResourceEvent event,
            Context context) throws InvalidImageReferenceException, InterruptedException, RegistryException, IOException, CacheDirectoryCreationException, ExecutionException {
        if (!Set.of("Create", "Update").contains(event.getRequestType())) {
            return Map.of();
        }

        Map<String, Object> properties = event.getResourceProperties();
        String source = (String) properties.get("source");
        String target = (String) properties.get("target");

        JibContainer container = Jib.from(registryImage(source, sourceCredentialRetriever))
                .containerize(configure(Containerizer.to(registryImage(target, targetCredentialRetriever))));

        return Map.of("Data", Map.of("digest", container.getDigest().toString()));
    }

    private static RegistryImage registryImage(String imageName, CredentialRetriever credentialRetriever) throws InvalidImageReferenceException {
        RegistryImage image = RegistryImage.named(imageName);
        if (credentialRetriever != null) {
            image.addCredentialRetriever(credentialRetriever);
        }
        return image;
    }

    private Containerizer configure(Containerizer containerizer) {
        return JibEvents.logEvents(LOGGER, containerizer.setAllowInsecureRegistries(allowInsecureRegistries));
    }

    public static class Builder {
        private boolean allowInsecureRegistries;
        private CredentialRetriever sourceCredentialRetriever;
        private CredentialRetriever targetCredentialRetriever;

        public Builder allowInsecureRegistries(boolean allowInsecureRegistries) {
            this.allowInsecureRegistries = allowInsecureRegistries;
            return this;
        }

        public Builder sourceCredentialRetriever(CredentialRetriever sourceCredentialRetriever) {
            this.sourceCredentialRetriever = sourceCredentialRetriever;
            return this;
        }

        public Builder targetCredentialRetriever(CredentialRetriever targetCredentialRetriever) {
            this.targetCredentialRetriever = targetCredentialRetriever;
            return this;
        }

        public UploadDockerImageLambda build() {
            return new UploadDockerImageLambda(this);
        }
    }

}
