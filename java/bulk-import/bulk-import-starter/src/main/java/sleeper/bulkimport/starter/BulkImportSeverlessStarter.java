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
package sleeper.bulkimport.starter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.bulkimport.starter.executor.Executor;
import sleeper.bulkimport.starter.executor.ExecutorFactory;

import java.io.IOException;

/**
 * The {@link BulkImportSeverlessStarter} consumes {@link sleeper.bulkimport.job.BulkImportJob} messages from SQS and starts executes them using
 * an {@link Executor}.
 */
public class BulkImportSeverlessStarter extends AbstractBulkImportStarter {
    public BulkImportSeverlessStarter() throws IOException {
        this(AmazonS3ClientBuilder.defaultClient(),
                EmrServerlessClient.builder().build(),
                AWSStepFunctionsClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient());
        }

    public BulkImportSeverlessStarter(AmazonS3 s3Client, EmrServerlessClient emrClient,
                             AWSStepFunctions stepFunctionsClient, AmazonDynamoDB dynamoDB) throws IOException {
        super(new ExecutorFactory(s3Client, emrClient, stepFunctionsClient, dynamoDB).createExecutor());
    }


    public BulkImportSeverlessStarter(Executor executor) {
        super(executor);
    }

}
