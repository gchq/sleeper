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

package sleeper.systemtest.drivers.util;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.clients.api.role.AssumeSleeperRole;
import sleeper.clients.api.role.AssumeSleeperRoleAwsSdk;
import sleeper.clients.api.role.AssumeSleeperRoleHadoop;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class SystemTestClients {
    private final AwsCredentialsProvider credentialsProvider;
    private final AwsRegionProvider regionProvider;
    private final S3Client s3;
    private final S3AsyncClient s3Async;
    private final S3TransferManager s3TransferManager;
    private final DynamoDbClient dynamo;
    private final StsClient sts;
    private final SqsClient sqs;
    private final LambdaClient lambda;
    private final CloudFormationClient cloudFormation;
    private final EmrServerlessClient emrServerless;
    private final EmrClient emr;
    private final EcsClient ecs;
    private final AutoScalingClient autoScaling;
    private final EcrClient ecr;
    private final Ec2Client ec2;
    private final CloudWatchClient cloudWatch;
    private final CloudWatchLogsClient cloudWatchLogs;
    private final CloudWatchEventsClient cloudWatchEvents;
    private final Supplier<Map<String, String>> getAuthEnvVars;
    private final UnaryOperator<Configuration> configureHadoop;
    private final boolean skipAssumeRole;

    private SystemTestClients(Builder builder) {
        credentialsProvider = builder.credentialsProvider;
        regionProvider = builder.regionProvider;
        s3 = builder.s3;
        s3Async = builder.s3Async;
        s3TransferManager = builder.s3TransferManager;
        dynamo = builder.dynamo;
        sts = builder.sts;
        sqs = builder.sqs;
        lambda = builder.lambda;
        cloudFormation = builder.cloudFormation;
        emrServerless = builder.emrServerless;
        emr = builder.emr;
        ecs = builder.ecs;
        autoScaling = builder.autoScaling;
        ecr = builder.ecr;
        ec2 = builder.ec2;
        cloudWatch = builder.cloudWatch;
        cloudWatchLogs = builder.cloudWatchLogs;
        cloudWatchEvents = builder.cloudWatchEvents;
        getAuthEnvVars = builder.getAuthEnvVars;
        configureHadoop = builder.configureHadoop;
        skipAssumeRole = builder.skipAssumeRole;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SystemTestClients fromDefaults() {
        return builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .regionProvider(DefaultAwsRegionProviderChain.builder().build())
                .s3(S3Client.create())
                .s3Async(S3AsyncClient.crtCreate())
                .dynamo(DynamoDbClient.create())
                .sts(StsClient.create())
                .sqs(SqsClient.create())
                .lambda(systemTestLambdaClientBuilder().build())
                .cloudFormation(CloudFormationClient.create())
                .emrServerless(EmrServerlessClient.create())
                .emr(EmrClient.create())
                .ecs(EcsClient.create())
                .autoScaling(AutoScalingClient.create())
                .ecr(EcrClient.create())
                .ec2(Ec2Client.create())
                .cloudWatch(CloudWatchClient.create())
                .cloudWatchLogs(CloudWatchLogsClient.create())
                .cloudWatchEvents(CloudWatchEventsClient.create())
                .build();
    }

    public SystemTestClients assumeRole(AssumeSleeperRole assumeRole) {
        if (skipAssumeRole) {
            return this;
        }
        AssumeSleeperRoleAwsSdk aws = assumeRole.forAwsSdk(sts);
        AssumeSleeperRoleHadoop hadoop = assumeRole.forHadoop();
        return builder()
                .credentialsProvider(aws.credentialsProvider())
                .regionProvider(regionProvider)
                .s3(aws.buildClient(S3Client.builder()))
                .s3Async(aws.buildClient(S3AsyncClient.crtBuilder()))
                .dynamo(aws.buildClient(DynamoDbClient.builder()))
                .sts(aws.buildClient(StsClient.builder()))
                .sqs(aws.buildClient(SqsClient.builder()))
                .lambda(aws.buildClient(systemTestLambdaClientBuilder()))
                .cloudFormation(aws.buildClient(CloudFormationClient.builder()))
                .emrServerless(aws.buildClient(EmrServerlessClient.builder()))
                .emr(aws.buildClient(EmrClient.builder()))
                .ecs(aws.buildClient(EcsClient.builder()))
                .autoScaling(aws.buildClient(AutoScalingClient.builder()))
                .ecr(aws.buildClient(EcrClient.builder()))
                .ec2(aws.buildClient(Ec2Client.builder()))
                .cloudWatch(aws.buildClient(CloudWatchClient.builder()))
                .cloudWatchLogs(aws.buildClient(CloudWatchLogsClient.builder()))
                .cloudWatchEvents(aws.buildClient(CloudWatchEventsClient.builder()))
                .getAuthEnvVars(aws::authEnvVars)
                .configureHadoop(hadoop::setS3ACredentials)
                .build();
    }

    public AwsCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public AwsRegionProvider getRegionProvider() {
        return regionProvider;
    }

    public S3Client getS3() {
        return s3;
    }

    public S3AsyncClient getS3Async() {
        return s3Async;
    }

    public S3TransferManager getS3TransferManager() {
        return s3TransferManager;
    }

    public DynamoDbClient getDynamo() {
        return dynamo;
    }

    public StsClient getSts() {
        return sts;
    }

    public SqsClient getSqs() {
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

    public EmrClient getEmr() {
        return emr;
    }

    public EcsClient getEcs() {
        return ecs;
    }

    public AutoScalingClient getAutoScaling() {
        return autoScaling;
    }

    public EcrClient getEcr() {
        return ecr;
    }

    public Ec2Client getEc2() {
        return ec2;
    }

    public CloudWatchClient getCloudWatch() {
        return cloudWatch;
    }

    public CloudWatchLogsClient getCloudWatchLogs() {
        return cloudWatchLogs;
    }

    public CloudWatchEventsClient getCloudWatchEvents() {
        return cloudWatchEvents;
    }

    public Map<String, String> getAuthEnvVars() {
        return getAuthEnvVars.get();
    }

    public Configuration createHadoopConf() {
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

    public static class Builder {
        private AwsCredentialsProvider credentialsProvider;
        private AwsRegionProvider regionProvider;
        private S3Client s3;
        private S3AsyncClient s3Async;
        private S3TransferManager s3TransferManager;
        private DynamoDbClient dynamo;
        private StsClient sts;
        private SqsClient sqs;
        private LambdaClient lambda;
        private CloudFormationClient cloudFormation;
        private EmrServerlessClient emrServerless;
        private EmrClient emr;
        private EcsClient ecs;
        private AutoScalingClient autoScaling;
        private EcrClient ecr;
        private Ec2Client ec2;
        private CloudWatchClient cloudWatch;
        private CloudWatchLogsClient cloudWatchLogs;
        private CloudWatchEventsClient cloudWatchEvents;
        private Supplier<Map<String, String>> getAuthEnvVars = Map::of;
        private UnaryOperator<Configuration> configureHadoop = conf -> conf;
        private boolean skipAssumeRole = false;

        private Builder() {
        }

        public Builder credentialsProvider(AwsCredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder s3(S3Client s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3Async(S3AsyncClient s3Async) {
            this.s3Async = s3Async;
            this.s3TransferManager = S3TransferManager.builder().s3Client(s3Async).build();
            return this;
        }

        public Builder dynamo(DynamoDbClient dynamo) {
            this.dynamo = dynamo;
            return this;
        }

        public Builder sts(StsClient sts) {
            this.sts = sts;
            return this;
        }

        public Builder sqs(SqsClient sqs) {
            this.sqs = sqs;
            return this;
        }

        public Builder lambda(LambdaClient lambda) {
            this.lambda = lambda;
            return this;
        }

        public Builder cloudFormation(CloudFormationClient cloudFormation) {
            this.cloudFormation = cloudFormation;
            return this;
        }

        public Builder emrServerless(EmrServerlessClient emrServerless) {
            this.emrServerless = emrServerless;
            return this;
        }

        public Builder emr(EmrClient emr) {
            this.emr = emr;
            return this;
        }

        public Builder ecs(EcsClient ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder autoScaling(AutoScalingClient autoScaling) {
            this.autoScaling = autoScaling;
            return this;
        }

        public Builder ecr(EcrClient ecr) {
            this.ecr = ecr;
            return this;
        }

        public Builder ec2(Ec2Client ec2) {
            this.ec2 = ec2;
            return this;
        }

        public Builder cloudWatch(CloudWatchClient cloudWatch) {
            this.cloudWatch = cloudWatch;
            return this;
        }

        public Builder cloudWatchLogs(CloudWatchLogsClient cloudWatchLogs) {
            this.cloudWatchLogs = cloudWatchLogs;
            return this;
        }

        public Builder cloudWatchEvents(CloudWatchEventsClient cloudWatchEvents) {
            this.cloudWatchEvents = cloudWatchEvents;
            return this;
        }

        public Builder getAuthEnvVars(Supplier<Map<String, String>> getAuthEnvVars) {
            this.getAuthEnvVars = getAuthEnvVars;
            return this;
        }

        public Builder configureHadoop(UnaryOperator<Configuration> configureHadoop) {
            this.configureHadoop = configureHadoop;
            return this;
        }

        public Builder configureHadoopSetter(Consumer<Configuration> configureHadoop) {
            this.configureHadoop = conf -> {
                configureHadoop.accept(conf);
                return conf;
            };
            return this;
        }

        public Builder skipAssumeRole(boolean skipAssumeRole) {
            this.skipAssumeRole = skipAssumeRole;
            return this;
        }

        public SystemTestClients build() {
            return new SystemTestClients(this);
        }

    }
}
