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
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.util.AssumeSleeperRoleHadoop;
import sleeper.clients.util.AssumeSleeperRoleNew;
import sleeper.clients.util.AssumeSleeperRoleV1;
import sleeper.clients.util.AssumeSleeperRoleV2;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;

public class SystemTestClients {
    private final AmazonS3 s3;
    private final S3Client s3V2;
    private final AmazonDynamoDB dynamoDB;
    private final AWSSecurityTokenService sts;
    private final StsClient stsV2;
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
    private final UnaryOperator<Configuration> configureHadoop;

    public SystemTestClients() {
        s3 = AmazonS3ClientBuilder.defaultClient();
        s3V2 = S3Client.create();
        dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
        stsV2 = StsClient.create();
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
        configureHadoop = conf -> conf;
    }

    public SystemTestClients(SystemTestClients clients, AssumeSleeperRoleNew assumeRole) {
        AssumeSleeperRoleV1 v1 = assumeRole.forAwsV1(clients.sts);
        AssumeSleeperRoleV2 v2 = assumeRole.forAwsV2(clients.stsV2);
        AssumeSleeperRoleHadoop hadoop = assumeRole.forHadoop();
        s3 = v1.buildClient(AmazonS3ClientBuilder.standard());
        s3V2 = v2.buildClient(S3Client.builder());
        dynamoDB = v1.buildClient(AmazonDynamoDBClientBuilder.standard());
        sts = v1.buildClient(AWSSecurityTokenServiceClientBuilder.standard());
        stsV2 = v2.buildClient(StsClient.builder());
        regionProvider = v2.regionProvider();
        sqs = v1.buildClient(AmazonSQSClientBuilder.standard());
        lambda = v2.buildClient(systemTestLambdaClientBuilder());
        cloudFormation = v2.buildClient(CloudFormationClient.builder());
        emrServerless = v2.buildClient(EmrServerlessClient.builder());
        emr = v1.buildClient(AmazonElasticMapReduceClientBuilder.standard());
        ecs = v1.buildClient(AmazonECSClientBuilder.standard());
        autoScaling = v1.buildClient(AmazonAutoScalingClientBuilder.standard());
        ecr = v1.buildClient(AmazonECRClientBuilder.standard());
        cloudWatch = v2.buildClient(CloudWatchClient.builder());
        cloudWatchEvents = v1.buildClient(AmazonCloudWatchEventsClientBuilder.standard());
        authEnvVars = v1.authEnvVars();
        configureHadoop = hadoop::setS3ACredentials;
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

    public Configuration createHadoopConf() {
        // Not applying instance admin credentials, as this produced an intermittent AWSBadRequestException during the
        // getFileStatus call from ParquetReader.Builder.build.
        return configureHadoop.apply(HadoopConfigurationProvider.getConfigurationForClient());
    }

    public Configuration createHadoopConf(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return configureHadoop.apply(HadoopConfigurationProvider.getConfigurationForClient(instanceProperties, tableProperties));
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
