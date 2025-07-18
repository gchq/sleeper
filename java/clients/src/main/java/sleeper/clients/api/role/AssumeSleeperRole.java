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
package sleeper.clients.api.role;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;

import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ADMIN_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_DIRECT_ROLE_ARN;
import static sleeper.core.properties.instance.CommonProperty.ENDPOINT_URL;
import static sleeper.core.properties.instance.CommonProperty.REGION;

public class AssumeSleeperRole {
    public static final Logger LOGGER = LoggerFactory.getLogger(AssumeSleeperRole.class);

    private final String region;
    private final String endpointUrl;
    private final String roleArn;
    private final String roleSessionName;

    private AssumeSleeperRole(String region, String endpointUrl, String roleArn) {
        this(region, endpointUrl, roleArn, UUID.randomUUID().toString());
    }

    private AssumeSleeperRole(
            String region, String endpointUrl, String roleArn, String roleSessionName) {
        this.region = region;
        this.endpointUrl = endpointUrl;
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        LOGGER.info("Assuming role: {}", roleArn);
    }

    public static AssumeSleeperRole ingestByQueue(InstanceProperties instanceProperties) {
        return fromArnProperty(instanceProperties, INGEST_BY_QUEUE_ROLE_ARN);
    }

    public static AssumeSleeperRole directIngest(InstanceProperties instanceProperties) {
        return fromArnProperty(instanceProperties, INGEST_DIRECT_ROLE_ARN);
    }

    public static AssumeSleeperRole instanceAdmin(InstanceProperties instanceProperties) {
        return fromArnProperty(instanceProperties, ADMIN_ROLE_ARN);
    }

    private static AssumeSleeperRole fromArnProperty(
            InstanceProperties instanceProperties, InstanceProperty roleArnProperty) {
        String region = instanceProperties.get(REGION);
        String endpointUrl = instanceProperties.get(ENDPOINT_URL);
        String roleArn = instanceProperties.get(roleArnProperty);
        return new AssumeSleeperRole(region, endpointUrl, roleArn);
    }

    public static AssumeSleeperRole fromArn(String roleArn) {
        String region = new DefaultAwsRegionProviderChain().getRegion().id();
        return new AssumeSleeperRole(region, null, roleArn);
    }

    public AssumeSleeperRoleAwsSdk forAwsSdk(StsClient sts) {
        StsAssumeRoleCredentialsProvider provider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(builder -> builder.roleArn(roleArn).roleSessionName(roleSessionName))
                .stsClient(sts)
                .build();
        return new AssumeSleeperRoleAwsSdk(region, endpointUrl, provider);
    }

    public AssumeSleeperRoleHadoop forHadoop() {
        return new AssumeSleeperRoleHadoop(roleArn, roleSessionName);
    }
}
