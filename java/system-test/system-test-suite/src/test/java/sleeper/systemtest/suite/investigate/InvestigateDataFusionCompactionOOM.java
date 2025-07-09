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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.rust.RustCompactionRunner;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionTree;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * This was an investigation after a failed system test where compactions ran out of memory unexpectedly.
 */
public class InvestigateDataFusionCompactionOOM {
    public static final Logger LOGGER = LoggerFactory.getLogger(InvestigateDataFusionCompactionOOM.class);

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

    private static void examine(CheckTransactionLogs logs, S3Client s3Client, DynamoDbClient dynamoClient) throws IOException, IteratorCreationException {
        PartitionTree partitionTree = logs.partitionTree();
        Path tempDir = Files.createTempDirectory("sleeper-test");
        Path outputFile = tempDir.resolve(UUID.randomUUID().toString());
        CompactionJob actualJob = logs.inferLastCompactionJobFromAssignJobIdsTransaction();
        CompactionJobSerDe serDe = new CompactionJobSerDe();
        LOGGER.info("Found compaction job from last assign job IDs transaction: {}", serDe.toJson(actualJob));
        CompactionJob job = logs.lastCompactionJobFromAssignJobIdsTransactionToLocalFile(outputFile);
        CompactionRunner runner = new RustCompactionRunner();
        runner.compact(job, logs.tableProperties(), partitionTree.getPartition(job.getPartitionId()));
    }

}
