/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.clients.util;

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;

import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ADMIN_ROLE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_DIRECT_ROLE_ARN;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class AssumeSleeperRoleNew {
    private final String region;
    private final String roleArn;
    private final String roleSessionName;

    private AssumeSleeperRoleNew(String region, String roleArn) {
        this(region, roleArn, UUID.randomUUID().toString());
    }

    private AssumeSleeperRoleNew(
            String region, String roleArn, String roleSessionName) {
        this.region = region;
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
    }

    public static AssumeSleeperRoleNew ingestByQueue(InstanceProperties instanceProperties) {
        return fromArnProperty(instanceProperties, INGEST_BY_QUEUE_ROLE_ARN);
    }

    public static AssumeSleeperRoleNew directIngest(InstanceProperties instanceProperties) {
        return fromArnProperty(instanceProperties, INGEST_DIRECT_ROLE_ARN);
    }

    public static AssumeSleeperRoleNew instanceAdmin(InstanceProperties instanceProperties) {
        return fromArnProperty(instanceProperties, ADMIN_ROLE_ARN);
    }

    private static AssumeSleeperRoleNew fromArnProperty(
            InstanceProperties instanceProperties, InstanceProperty roleArnProperty) {
        String region = instanceProperties.get(REGION);
        String roleArn = instanceProperties.get(roleArnProperty);
        return new AssumeSleeperRoleNew(region, roleArn);
    }

    public static AssumeSleeperRoleNew fromArn(String roleArn) {
        String region = new DefaultAwsRegionProviderChain().getRegion().id();
        return new AssumeSleeperRoleNew(region, roleArn);
    }

    public AssumeSleeperRoleV1 forAwsV1(AWSSecurityTokenService sts) {
        STSAssumeRoleSessionCredentialsProvider provider = new STSAssumeRoleSessionCredentialsProvider.Builder(
                roleArn, roleSessionName)
                .withStsClient(sts)
                .build();
        return new AssumeSleeperRoleV1(region, provider);
    }

    public AssumeSleeperRoleV2 forAwsV2(StsClient sts) {
        StsAssumeRoleCredentialsProvider provider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(builder -> builder.roleArn(roleArn).roleSessionName(roleSessionName))
                .stsClient(sts)
                .build();
        return new AssumeSleeperRoleV2(region, provider);
    }

    public AssumeSleeperRoleHadoop forHadoop() {
        return new AssumeSleeperRoleHadoop(roleArn, roleSessionName);
    }
}
