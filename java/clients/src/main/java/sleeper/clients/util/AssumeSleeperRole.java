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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;

import java.util.Map;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ADMIN_ROLE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_DIRECT_ROLE_ARN;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class AssumeSleeperRole {

    public static final Logger LOGGER = LoggerFactory.getLogger(AssumeSleeperRole.class);

    private final String region;
    private final String roleArn;
    private final String roleSessionName;
    private final AWSCredentialsProvider providerV1;
    private final AwsCredentialsProvider providerV2;

    private AssumeSleeperRole(
            String region, String roleArn, String roleSessionName,
            AWSCredentialsProvider providerV1, AwsCredentialsProvider providerV2) {
        this.region = region;
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        this.providerV1 = providerV1;
        this.providerV2 = providerV2;
    }

    public static AssumeSleeperRole ingestByQueue(
            AWSSecurityTokenService sts, InstanceProperties instanceProperties) {
        return fromArnProperty(sts, instanceProperties, INGEST_BY_QUEUE_ROLE_ARN);
    }

    public static AssumeSleeperRole directIngest(
            AWSSecurityTokenService sts, InstanceProperties instanceProperties) {
        return fromArnProperty(sts, instanceProperties, INGEST_DIRECT_ROLE_ARN);
    }

    public static AssumeSleeperRole instanceAdmin(
            AWSSecurityTokenService sts, InstanceProperties instanceProperties) {
        return fromArnProperty(sts, instanceProperties, ADMIN_ROLE_ARN);
    }

    private static AssumeSleeperRole fromArnProperty(
            AWSSecurityTokenService sts, InstanceProperties instanceProperties, InstanceProperty roleArnProperty) {
        String region = instanceProperties.get(REGION);
        String roleArn = instanceProperties.get(roleArnProperty);
        return fromRegionAndArn(sts, region, roleArn);
    }

    public static AssumeSleeperRole fromArn(AWSSecurityTokenService sts, String roleArn) {
        String region = new DefaultAwsRegionProviderChain().getRegion().id();
        return fromRegionAndArn(sts, region, roleArn);
    }

    public static AssumeSleeperRole fromRegionAndArnNew(AWSSecurityTokenService stsV1, StsClient stsV2, String region, String roleArn) {
        LOGGER.info("Assuming instance role: {}", roleArn);
        String roleSessionName = UUID.randomUUID().toString();
        STSAssumeRoleSessionCredentialsProvider providerV1 = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
                .withStsClient(stsV1)
                .build();
        StsAssumeRoleCredentialsProvider providerV2 = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(builder -> builder.roleArn(roleArn).roleSessionName(roleSessionName))
                .stsClient(stsV2)
                .build();

        return new AssumeSleeperRole(region, roleArn, roleSessionName, providerV1, providerV2);
    }

    public static AssumeSleeperRole fromRegionAndArn(AWSSecurityTokenService sts, String region, String roleArn) {
        LOGGER.info("Assuming instance role: {}", roleArn);
        String roleSessionName = UUID.randomUUID().toString();
        AssumeRoleResult result = sts.assumeRole(new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withRoleSessionName(roleSessionName));
        Credentials credentials = result.getCredentials();

        AWSCredentialsProvider providerV1 = new AWSStaticCredentialsProvider(new BasicSessionCredentials(
                credentials.getAccessKeyId(), credentials.getSecretAccessKey(), credentials.getSessionToken()));
        AwsCredentialsProvider providerV2 = StaticCredentialsProvider.create(AwsSessionCredentials.create(
                credentials.getAccessKeyId(), credentials.getSecretAccessKey(), credentials.getSessionToken()));
        return new AssumeSleeperRole(region, roleArn, roleSessionName, providerV1, providerV2);
    }

    public <T, B extends com.amazonaws.client.builder.AwsClientBuilder<B, T>> T v1Client(B builder) {
        return builder.withCredentials(providerV1).withRegion(region).build();
    }

    public <T, B extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<B, T>> T v2Client(B builder) {
        return builder.credentialsProvider(providerV2).region(Region.of(region)).build();
    }

    public Configuration setS3ACredentials(Configuration configuration) {
        String originalCredentialsProvider = configuration.get("fs.s3a.aws.credentials.provider");
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider");
        configuration.set("fs.s3a.assumed.role.arn", roleArn);
        configuration.set("fs.s3a.assumed.role.session.name", roleSessionName);
        configuration.set("fs.s3a.assumed.role.credentials.provider", originalCredentialsProvider);
        return configuration;
    }

    public AwsRegionProvider v2RegionProvider() {
        return () -> Region.of(region);
    }

    public Map<String, String> authEnvVars() {
        AwsSessionCredentials credentials = (AwsSessionCredentials) providerV2.resolveCredentials();
        return Map.of(
                "AWS_ACCESS_KEY_ID", credentials.accessKeyId(),
                "AWS_SECRET_ACCESS_KEY", credentials.secretAccessKey(),
                "AWS_SESSION_TOKEN", credentials.sessionToken(),
                "AWS_REGION", region,
                "AWS_DEFAULT_REGION", region);
    }

}
