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
package sleeper.systemtest.drivers.util;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_CA_DATA;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_ENDPOINT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_CLUSTER_NAME;

/**
 * Creates Kubernetes clients to talk to an EKS cluster.
 */
public class EksClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(EksClientFactory.class);

    private EksClientFactory() {
    }

    public static KubernetesClient createKubernetesClient(InstanceProperties instanceProperties, Region region, AwsCredentialsProvider credentialsProvider) {
        String clusterName = instanceProperties.get(BULK_IMPORT_EKS_CLUSTER_NAME);
        String endpoint = instanceProperties.get(BULK_IMPORT_EKS_CLUSTER_ENDPOINT);
        String caData = instanceProperties.get(BULK_IMPORT_EKS_CLUSTER_CA_DATA);
        LOGGER.info("Connecting to Kubernetes cluster {} at endpoint: {}", clusterName, endpoint);
        Config config = new ConfigBuilder()
                .withMasterUrl(endpoint)
                .withCaCertData(caData)
                .withOauthTokenProvider(() -> EksAuthTokenGenerator.generateToken(clusterName, region, credentialsProvider))
                .withConnectionTimeout(30 * 1000)
                .withRequestTimeout(60 * 1000)
                .build();
        return new KubernetesClientBuilder().withConfig(config).build();
    }

}
