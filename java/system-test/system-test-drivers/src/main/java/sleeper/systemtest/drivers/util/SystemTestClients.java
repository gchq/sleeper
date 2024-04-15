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

package sleeper.systemtest.drivers.util;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.AssumeAdminRole;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.time.Duration;

public class SystemTestClients {
    private final AmazonS3 s3;
    private final S3Client s3V2;
    private final AmazonDynamoDB dynamoDB;
    private final AWSSecurityTokenService sts;
    private final AwsRegionProvider regionProvider;
    private final AmazonSQS sqs;
    private final LambdaClient lambda;
    private final CloudFormationClient cloudFormation;
    private final EmrServerlessClient emrServerless;
    private final AmazonElasticMapReduce emr;
    private final AmazonECS ecs;
    private final AmazonAutoScaling autoScaling;
    private final AmazonECR ecr;
    private final CloudWatchClient cloudWatch;

    public SystemTestClients() {
        s3 = AmazonS3ClientBuilder.defaultClient();
        s3V2 = S3Client.create();
        dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
        regionProvider = DefaultAwsRegionProviderChain.builder().build();
        sqs = AmazonSQSClientBuilder.defaultClient();
        lambda = systemTestLambdaClientBuilder().build();
        cloudFormation = CloudFormationClient.create();
        emrServerless = EmrServerlessClient.create();
        emr = AmazonElasticMapReduceClientBuilder.defaultClient();
        ecs = AmazonECSClientBuilder.defaultClient();
        autoScaling = AmazonAutoScalingClientBuilder.defaultClient();
        ecr = AmazonECRClientBuilder.defaultClient();
        cloudWatch = CloudWatchClient.create();
    }

    private SystemTestClients(AWSCredentialsProvider credentialsV1, AwsCredentialsProvider credentialsV2, AwsRegionProvider regionProvider) {
        s3 = v1Client(AmazonS3ClientBuilder.standard(), credentialsV1);
        s3V2 = v2Client(S3Client.builder(), credentialsV2);
        dynamoDB = v1Client(AmazonDynamoDBClientBuilder.standard(), credentialsV1);
        sts = v1Client(AWSSecurityTokenServiceClientBuilder.standard(), credentialsV1);
        this.regionProvider = regionProvider;
        sqs = v1Client(AmazonSQSClientBuilder.standard(), credentialsV1);
        lambda = v2Client(systemTestLambdaClientBuilder(), credentialsV2);
        cloudFormation = v2Client(CloudFormationClient.builder(), credentialsV2);
        emrServerless = v2Client(EmrServerlessClient.builder(), credentialsV2);
        emr = v1Client(AmazonElasticMapReduceClientBuilder.standard(), credentialsV1);
        ecs = v1Client(AmazonECSClientBuilder.standard(), credentialsV1);
        autoScaling = v1Client(AmazonAutoScalingClientBuilder.standard(), credentialsV1);
        ecr = v1Client(AmazonECRClientBuilder.standard(), credentialsV1);
        cloudWatch = v2Client(CloudWatchClient.builder(), credentialsV2);
    }

    public SystemTestClients assumeAdminRole(InstanceProperties instanceProperties) {
        AssumeAdminRole assumeRole = AssumeAdminRole.authForInstance(sts, instanceProperties);
        return new SystemTestClients(assumeRole.credentialsV1(), assumeRole.credentialsV2(), regionProvider);
    }

    public AmazonS3 getS3() {
        return s3;
    }

    public S3Client getS3V2() {
        return s3V2;
    }

    public AmazonDynamoDB getDynamoDB() {
        return dynamoDB;
    }

    public AWSSecurityTokenService getSts() {
        return sts;
    }

    public AwsRegionProvider getRegionProvider() {
        return regionProvider;
    }

    public AmazonSQS getSqs() {
        return sqs;
    }

    public LambdaClient getLambda() {
        return lambda;
    }

    public CloudFormationClient getCloudFormation() {
        return cloudFormation;
    }

    public EmrServerlessClient getEmrServerless() {
        return emrServerless;
    }

    public AmazonElasticMapReduce getEmr() {
        return emr;
    }

    public AmazonECS getEcs() {
        return ecs;
    }

    public AmazonAutoScaling getAutoScaling() {
        return autoScaling;
    }

    public AmazonECR getEcr() {
        return ecr;
    }

    public CloudWatchClient getCloudWatch() {
        return cloudWatch;
    }

    private static <T, B extends com.amazonaws.client.builder.AwsClientBuilder<B, T>> T v1Client(
            B builder, AWSCredentialsProvider credentials) {
        return builder.withCredentials(credentials).build();
    }

    private static <T, B extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<B, T>> T v2Client(
            B builder, AwsCredentialsProvider credentials) {
        return builder.credentialsProvider(credentials).build();
    }

    private static LambdaClientBuilder systemTestLambdaClientBuilder() {
        return LambdaClient.builder()
                .overrideConfiguration(builder -> builder
                        .apiCallTimeout(Duration.ofMinutes(11))
                        .apiCallAttemptTimeout(Duration.ofMinutes(11)))
                .httpClientBuilder(ApacheHttpClient.builder()
                        .socketTimeout(Duration.ofMinutes(11)));
    }
}
