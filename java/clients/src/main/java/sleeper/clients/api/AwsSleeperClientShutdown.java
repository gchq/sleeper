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
package sleeper.clients.api;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;

import java.util.concurrent.ExecutorService;

public class AwsSleeperClientShutdown implements AutoCloseable {
    private final ExecutorService executorService;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final AmazonSQS sqsClient;
    private final boolean shutdownClients;

    public AwsSleeperClientShutdown(ExecutorService executorService, AmazonS3 s3Client, AmazonDynamoDB dynamoClient, AmazonSQS sqsClient, boolean shutdownClients) {
        this.executorService = executorService;
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.sqsClient = sqsClient;
        this.shutdownClients = shutdownClients;
    }

    @Override
    public void close() throws Exception {
        executorService.shutdown();
        if (shutdownClients) {
            s3Client.shutdown();
            dynamoClient.shutdown();
            sqsClient.shutdown();
        }
    }

}
