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

    private final Credentials credentials;
    private final String region;
    private final AWSCredentialsProvider credentialsV1;
    private final AwsCredentialsProvider credentialsV2;

    private AssumeSleeperRole(
            Credentials credentials, String region,
            AWSCredentialsProvider credentialsV1, AwsCredentialsProvider credentialsV2) {
        this.credentials = credentials;
        this.region = region;
        this.credentialsV1 = credentialsV1;
        this.credentialsV2 = credentialsV2;
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

    public static AssumeSleeperRole fromRegionAndArn(AWSSecurityTokenService sts, String region, String roleArn) {
        LOGGER.info("Assuming instance role: {}", roleArn);
        AssumeRoleResult result = sts.assumeRole(new AssumeRoleRequest()
                .withRoleArn(roleArn)
                .withRoleSessionName(UUID.randomUUID().toString()));
        Credentials credentials = result.getCredentials();

        AWSCredentialsProvider credentialsV1 = new AWSStaticCredentialsProvider(new BasicSessionCredentials(
                credentials.getAccessKeyId(), credentials.getSecretAccessKey(), credentials.getSessionToken()));
        AwsCredentialsProvider credentialsV2 = StaticCredentialsProvider.create(AwsSessionCredentials.create(
                credentials.getAccessKeyId(), credentials.getSecretAccessKey(), credentials.getSessionToken()));
        return new AssumeSleeperRole(credentials, region, credentialsV1, credentialsV2);
    }

    public <T, B extends com.amazonaws.client.builder.AwsClientBuilder<B, T>> T v1Client(B builder) {
        return builder.withCredentials(credentialsV1).withRegion(region).build();
    }

    public <T, B extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<B, T>> T v2Client(B builder) {
        return builder.credentialsProvider(credentialsV2).region(Region.of(region)).build();
    }

    public Configuration setS3ACredentials(Configuration configuration) {
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
        configuration.set("fs.s3a.access.key", credentials.getAccessKeyId());
        configuration.set("fs.s3a.secret.key", credentials.getSecretAccessKey());
        configuration.set("fs.s3a.session.token", credentials.getSessionToken());
        return configuration;
    }

    public AwsRegionProvider v2RegionProvider() {
        return () -> Region.of(region);
    }

    public Map<String, String> authEnvVars() {
        return Map.of(
                "AWS_ACCESS_KEY_ID", credentials.getAccessKeyId(),
                "AWS_SECRET_ACCESS_KEY", credentials.getSecretAccessKey(),
                "AWS_SESSION_TOKEN", credentials.getSessionToken(),
                "AWS_REGION", region,
                "AWS_DEFAULT_REGION", region);
    }

}
