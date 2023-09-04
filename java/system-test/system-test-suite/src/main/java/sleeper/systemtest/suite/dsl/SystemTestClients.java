/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.dsl;

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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;

import static sleeper.systemtest.drivers.util.InvokeSystemTestLambda.createSystemTestLambdaClient;

public class SystemTestClients {
    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private final S3Client s3V2 = S3Client.create();
    private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private final LambdaClient lambda = createSystemTestLambdaClient();
    private final CloudFormationClient cloudFormation = CloudFormationClient.create();
    private final EmrServerlessClient emrServerless = EmrServerlessClient.create();
    private final AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.defaultClient();
    private final AmazonECS ecs = AmazonECSClientBuilder.defaultClient();
    private final AmazonECR ecr = AmazonECRClientBuilder.defaultClient();

    public AmazonS3 getS3() {
        return s3;
    }

    public S3Client getS3V2() {
        return s3V2;
    }

    public AmazonDynamoDB getDynamoDB() {
        return dynamoDB;
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

    public AmazonECR getEcr() {
        return ecr;
    }
}
