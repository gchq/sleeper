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
package sleeper.clients.compaction;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.invoke.tables.InvokeForTables;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_QUEUE_URL;

/**
 * Command line client to trigger garbage collection for specified Sleeper tables.
 */
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

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            String queueUrl = instanceProperties.get(GARBAGE_COLLECTOR_QUEUE_URL);
            InvokeForTables.sendOneMessagePerTableByName(sqsClient, queueUrl, tableIndex, tableNames);
        }
    }
}
