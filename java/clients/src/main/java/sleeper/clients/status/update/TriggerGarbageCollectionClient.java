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
package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_QUEUE_URL;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_TABLE_BATCH_SIZE;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class TriggerGarbageCollectionClient {
    private TriggerGarbageCollectionClient() {
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: <instance-id> <table-names-as-args>");
            return;
        }
        String instanceId = args[0];
        List<String> tableNames = Stream.of(args).skip(1).collect(toUnmodifiableList());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
            TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            List<TableStatus> tables = tableNames.stream()
                    .map(name -> tableIndex.getTableByName(name)
                            .orElseThrow(() -> new IllegalArgumentException("Table not found: " + name)))
                    .collect(toUnmodifiableList());
            int batchSize = instanceProperties.getInt(GARBAGE_COLLECTOR_TABLE_BATCH_SIZE);
            String queueUrl = instanceProperties.get(GARBAGE_COLLECTOR_QUEUE_URL);
            InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();
            InvokeForTableRequest.forTables(tables.stream(), batchSize,
                    request -> sqsClient.sendMessage(queueUrl, serDe.toJson(request)));
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
            sqsClient.shutdown();
        }
    }
}
