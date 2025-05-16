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
package sleeper.clients.table;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.clients.api.IngestBatcherSender;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;

import java.util.List;

public class SendToIngestBatcher {

    private SendToIngestBatcher() {
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: <instanceId> <tableName> <file1> [<file2> ...]");
            System.exit(1);
        }
        String instanceId = args[0];
        String tableName = args[1];
        List<String> files = List.of(args).subList(2, args.length);

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            if (!tableIndex.getTableByName(tableName).isPresent()) {
                System.out.println("Table " + tableName + " not found in instance " + instanceId);
                System.exit(1);
            }
            IngestBatcherSender.toSqs(instanceProperties, sqsClient)
                    .submit(new IngestBatcherSubmitRequest(tableName, files));
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
            sqsClient.shutdown();
        }
    }

}
