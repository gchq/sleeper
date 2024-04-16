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
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

public class SystemTestClients {
    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private final S3Client s3V2 = S3Client.create();
    private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    private final AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
    private final AwsRegionProvider regionProvider = DefaultAwsRegionProviderChain.builder().build();
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private final LambdaClient lambda = createSystemTestLambdaClient();
    private final CloudFormationClient cloudFormation = CloudFormationClient.create();
    private final EmrServerlessClient emrServerless = EmrServerlessClient.create();
    private final AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.defaultClient();
    private final AmazonECS ecs = AmazonECSClientBuilder.defaultClient();
    private final AmazonAutoScaling autoScaling = AmazonAutoScalingClientBuilder.defaultClient();
    private final AmazonECR ecr = AmazonECRClientBuilder.defaultClient();
    private final CloudWatchClient cloudWatch = CloudWatchClient.create();

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

    private static LambdaClient createSystemTestLambdaClient() {
        return LambdaClient.builder()
                .overrideConfiguration(builder -> builder
                        .apiCallTimeout(Duration.ofMinutes(11))
                        .apiCallAttemptTimeout(Duration.ofMinutes(11)))
                .httpClientBuilder(ApacheHttpClient.builder()
                        .socketTimeout(Duration.ofMinutes(11)))
                .build();
    }
}
