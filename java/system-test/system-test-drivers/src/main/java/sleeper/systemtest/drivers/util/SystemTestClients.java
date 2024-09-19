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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
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
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.util.AssumeSleeperRole;
import sleeper.clients.util.AssumeSleeperRoleHadoop;
import sleeper.clients.util.AssumeSleeperRoleV1;
import sleeper.clients.util.AssumeSleeperRoleV2;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class SystemTestClients {
    private final AwsRegionProvider regionProvider;
    private final AmazonS3 s3;
    private final S3Client s3V2;
    private final S3AsyncClient s3Async;
    private final AmazonDynamoDB dynamoDB;
    private final AWSSecurityTokenService sts;
    private final StsClient stsV2;
    private final AmazonSQS sqs;
    private final LambdaClient lambda;
    private final CloudFormationClient cloudFormation;
    private final EmrServerlessClient emrServerless;
    private final EmrClient emr;
    private final AmazonECS ecs;
    private final AmazonAutoScaling autoScaling;
    private final EcrClient ecr;
    private final CloudWatchClient cloudWatch;
    private final CloudWatchLogsClient cloudWatchLogs;
    private final CloudWatchEventsClient cloudWatchEvents;
    private final Supplier<Map<String, String>> getAuthEnvVars;
    private final UnaryOperator<Configuration> configureHadoop;
    private final boolean skipAssumeRole;

    private SystemTestClients(Builder builder) {
        regionProvider = builder.regionProvider;
        s3 = builder.s3;
        s3V2 = builder.s3V2;
        s3Async = builder.s3Async;
        dynamoDB = builder.dynamoDB;
        sts = builder.sts;
        stsV2 = builder.stsV2;
        sqs = builder.sqs;
        lambda = builder.lambda;
        cloudFormation = builder.cloudFormation;
        emrServerless = builder.emrServerless;
        emr = builder.emr;
        ecs = builder.ecs;
        autoScaling = builder.autoScaling;
        ecr = builder.ecr;
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
                .regionProvider(DefaultAwsRegionProviderChain.builder().build())
                .s3(AmazonS3ClientBuilder.defaultClient())
                .s3V2(S3Client.create())
                .s3Async(S3AsyncClient.create())
                .dynamoDB(AmazonDynamoDBClientBuilder.defaultClient())
                .sts(AWSSecurityTokenServiceClientBuilder.defaultClient())
                .stsV2(StsClient.create())
                .sqs(AmazonSQSClientBuilder.defaultClient())
                .lambda(systemTestLambdaClientBuilder().build())
                .cloudFormation(CloudFormationClient.create())
                .emrServerless(EmrServerlessClient.create())
                .emr(EmrClient.create())
                .ecs(AmazonECSClientBuilder.defaultClient())
                .autoScaling(AmazonAutoScalingClientBuilder.defaultClient())
                .ecr(EcrClient.create())
                .cloudWatch(CloudWatchClient.create())
                .cloudWatchLogs(CloudWatchLogsClient.create())
                .cloudWatchEvents(CloudWatchEventsClient.create())
                .build();
    }

    public SystemTestClients assumeRole(AssumeSleeperRole assumeRole) {
        if (skipAssumeRole) {
            return this;
        }
        AssumeSleeperRoleV1 v1 = assumeRole.forAwsV1(sts);
        AssumeSleeperRoleV2 v2 = assumeRole.forAwsV2(stsV2);
        AssumeSleeperRoleHadoop hadoop = assumeRole.forHadoop();
        return builder()
                .regionProvider(regionProvider)
                .s3(v1.buildClient(AmazonS3ClientBuilder.standard()))
                .s3V2(v2.buildClient(S3Client.builder()))
                .s3Async(v2.buildClient(S3AsyncClient.builder()))
                .dynamoDB(v1.buildClient(AmazonDynamoDBClientBuilder.standard()))
                .sts(v1.buildClient(AWSSecurityTokenServiceClientBuilder.standard()))
                .stsV2(v2.buildClient(StsClient.builder()))
                .sqs(v1.buildClient(AmazonSQSClientBuilder.standard()))
                .lambda(v2.buildClient(systemTestLambdaClientBuilder()))
                .cloudFormation(v2.buildClient(CloudFormationClient.builder()))
                .emrServerless(v2.buildClient(EmrServerlessClient.builder()))
                .emr(v2.buildClient(EmrClient.builder()))
                .ecs(v1.buildClient(AmazonECSClientBuilder.standard()))
                .autoScaling(v1.buildClient(AmazonAutoScalingClientBuilder.standard()))
                .ecr(v2.buildClient(EcrClient.builder()))
                .cloudWatch(v2.buildClient(CloudWatchClient.builder()))
                .cloudWatchLogs(v2.buildClient(CloudWatchLogsClient.builder()))
                .cloudWatchEvents(v2.buildClient(CloudWatchEventsClient.builder()))
                .getAuthEnvVars(v1::authEnvVars)
                .configureHadoop(hadoop::setS3ACredentials)
                .build();
    }

    public AmazonS3 getS3() {
        return s3;
    }

    public S3Client getS3V2() {
        return s3V2;
    }

    public S3AsyncClient getS3Async() {
        return s3Async;
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

    public EmrClient getEmr() {
        return emr;
    }

    public AmazonECS getEcs() {
        return ecs;
    }

    public AmazonAutoScaling getAutoScaling() {
        return autoScaling;
    }

    public EcrClient getEcr() {
        return ecr;
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
        private AwsRegionProvider regionProvider;
        private AmazonS3 s3;
        private S3Client s3V2;
        private S3AsyncClient s3Async;
        private AmazonDynamoDB dynamoDB;
        private AWSSecurityTokenService sts;
        private StsClient stsV2;
        private AmazonSQS sqs;
        private LambdaClient lambda;
        private CloudFormationClient cloudFormation;
        private EmrServerlessClient emrServerless;
        private EmrClient emr;
        private AmazonECS ecs;
        private AmazonAutoScaling autoScaling;
        private EcrClient ecr;
        private CloudWatchClient cloudWatch;
        private CloudWatchLogsClient cloudWatchLogs;
        private CloudWatchEventsClient cloudWatchEvents;
        private Supplier<Map<String, String>> getAuthEnvVars = Map::of;
        private UnaryOperator<Configuration> configureHadoop = conf -> conf;
        private boolean skipAssumeRole = false;

        private Builder() {
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3V2(S3Client s3V2) {
            this.s3V2 = s3V2;
            return this;
        }

        public Builder s3Async(S3AsyncClient s3Async) {
            this.s3Async = s3Async;
            return this;
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public Builder sts(AWSSecurityTokenService sts) {
            this.sts = sts;
            return this;
        }

        public Builder stsV2(StsClient stsV2) {
            this.stsV2 = stsV2;
            return this;
        }

        public Builder sqs(AmazonSQS sqs) {
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

        public Builder ecs(AmazonECS ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder autoScaling(AmazonAutoScaling autoScaling) {
            this.autoScaling = autoScaling;
            return this;
        }

        public Builder ecr(EcrClient ecr) {
            this.ecr = ecr;
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
