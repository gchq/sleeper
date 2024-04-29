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

package sleeper.cdk.stack;

import software.amazon.awscdk.services.iam.IGrantable;

public final class StateStoreStacks {

    private final DynamoDBStateStoreStack dynamo;
    private final S3StateStoreStack s3;
    private final TransactionLogStateStoreStack transactionLog;

    public StateStoreStacks(
            DynamoDBStateStoreStack dynamo, S3StateStoreStack s3,
            TransactionLogStateStoreStack transactionLog,
            ManagedPoliciesStack policiesStack) {
        this.dynamo = dynamo;
        this.s3 = s3;
        this.transactionLog = transactionLog;
        grantReadPartitionsReadWriteActiveFiles(policiesStack.getIngestPolicy());
        grantReadActiveFilesAndPartitions(policiesStack.getQueryPolicy());
        grantReadActiveFilesAndPartitions(policiesStack.getReportingPolicy());
        grantReadWritePartitions(policiesStack.getEditTablesPolicy());
    }

    public void grantReadActiveFilesAndPartitions(IGrantable grantee) {
        dynamo.grantReadActiveFileMetadata(grantee);
        dynamo.grantReadPartitionMetadata(grantee);
        s3.grantRead(grantee);
        transactionLog.grantReadFiles(grantee);
        transactionLog.grantReadPartitions(grantee);
        transactionLog.grantReadSnapshots(grantee);
    }

    public void grantReadWriteAllFilesAndPartitions(IGrantable grantee) {
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWriteFiles(grantee);
        transactionLog.grantReadWritePartitions(grantee);
        transactionLog.grantReadWriteSnapshots(grantee);
    }

    public void grantReadActiveFilesReadWritePartitions(IGrantable grantee) {
        dynamo.grantReadActiveFileMetadata(grantee);
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadFiles(grantee);
        transactionLog.grantReadWritePartitions(grantee);
        transactionLog.grantReadWriteSnapshots(grantee);
    }

    public void grantReadPartitionsReadWriteActiveFiles(IGrantable grantee) {
        dynamo.grantReadPartitionMetadata(grantee);
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadPartitions(grantee);
        transactionLog.grantReadWriteFiles(grantee);
        transactionLog.grantReadWriteSnapshots(grantee);
    }

    public void grantReadPartitions(IGrantable grantee) {
        dynamo.grantReadPartitionMetadata(grantee);
        s3.grantRead(grantee);
        transactionLog.grantReadPartitions(grantee);
        transactionLog.grantReadSnapshots(grantee);
    }

    public void grantReadWriteActiveAndReadyForGCFiles(IGrantable grantee) {
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWriteFiles(grantee);
        transactionLog.grantReadWriteSnapshots(grantee);
    }

    public void grantReadWriteReadyForGCFiles(IGrantable grantee) {
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWriteFiles(grantee);
        transactionLog.grantReadWriteSnapshots(grantee);
    }

    public void grantReadWritePartitions(IGrantable grantee) {
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWritePartitions(grantee);
        transactionLog.grantReadWriteSnapshots(grantee);
    }
}
