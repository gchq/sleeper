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
package sleeper.systemtest.suite.investigate;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Objects;
import java.util.Optional;

/**
 * This was an investigation after a failed system test where compactions ran out of memory unexpectedly.
 */
public class InvestigateDataFusionCompactionOOM {

    private InvestigateDataFusionCompactionOOM() {
    }

    public static void main(String[] args) throws Exception {
        String instanceId = Objects.requireNonNull(System.getenv("INSTANCE_ID"), "INSTANCE_ID must be set");
        String tableId = Objects.requireNonNull(System.getenv("TABLE_ID"), "TABLE_ID must be set");
        boolean cacheTransactions = Optional.ofNullable(System.getenv("CACHE_TRANSACTIONS")).map(Boolean::parseBoolean).orElse(true);

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create()) {
            CheckTransactionLogs check = CheckTransactionLogs.load(instanceId, tableId, cacheTransactions, s3Client, dynamoClient);
            examine(check, s3Client, dynamoClient);
        }
    }

    private static void examine(CheckTransactionLogs logs, S3Client s3Client, DynamoDbClient dynamoClient) {
        // TODO
    }

}
