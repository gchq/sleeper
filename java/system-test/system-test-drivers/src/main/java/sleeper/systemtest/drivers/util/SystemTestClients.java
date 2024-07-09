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

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClientBuilder;
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
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.AssumeSleeperRole;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

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
    private final AmazonCloudWatchEvents cloudWatchEvents;
    private final Map<String, String> authEnvVars;
    private final Supplier<Configuration> hadoopConfSupplier;

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
        cloudWatchEvents = AmazonCloudWatchEventsClientBuilder.defaultClient();
        authEnvVars = Map.of();
        hadoopConfSupplier = HadoopConfigurationProvider::getConfigurationForClient;
    }

    public SystemTestClients(AssumeSleeperRole assumeRole) {
        s3 = assumeRole.v1Client(AmazonS3ClientBuilder.standard());
        s3V2 = assumeRole.v2Client(S3Client.builder());
        dynamoDB = assumeRole.v1Client(AmazonDynamoDBClientBuilder.standard());
        sts = assumeRole.v1Client(AWSSecurityTokenServiceClientBuilder.standard());
        regionProvider = assumeRole.v2RegionProvider();
        sqs = assumeRole.v1Client(AmazonSQSClientBuilder.standard());
        lambda = assumeRole.v2Client(systemTestLambdaClientBuilder());
        cloudFormation = assumeRole.v2Client(CloudFormationClient.builder());
        emrServerless = assumeRole.v2Client(EmrServerlessClient.builder());
        emr = assumeRole.v1Client(AmazonElasticMapReduceClientBuilder.standard());
        ecs = assumeRole.v1Client(AmazonECSClientBuilder.standard());
        autoScaling = assumeRole.v1Client(AmazonAutoScalingClientBuilder.standard());
        ecr = assumeRole.v1Client(AmazonECRClientBuilder.standard());
        cloudWatch = assumeRole.v2Client(CloudWatchClient.builder());
        cloudWatchEvents = assumeRole.v1Client(AmazonCloudWatchEventsClientBuilder.standard());
        authEnvVars = assumeRole.authEnvVars();
        hadoopConfSupplier = () -> assumeRole.setS3ACredentials(new Configuration());
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

    public AmazonCloudWatchEvents getCloudWatchEvents() {
        return cloudWatchEvents;
    }

    public Map<String, String> getAuthEnvVars() {
        return authEnvVars;
    }

    public Configuration getConfiguration() {
        return hadoopConfSupplier.get();
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
