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
        grantReadPartitionsReadWriteActiveFiles(policiesStack.getDirectIngestPolicyForGrants());
        grantReadActiveFilesAndPartitions(policiesStack.getQueryPolicyForGrants());
        grantReadActiveFilesAndPartitions(policiesStack.getReportingPolicyForGrants());
        grantReadWritePartitions(policiesStack.getEditTablesPolicyForGrants());
    }

    public void grantReadActiveFilesAndPartitions(IGrantable grantee) {
        dynamo.grantReadActiveFileMetadata(grantee);
        dynamo.grantReadPartitionMetadata(grantee);
        s3.grantRead(grantee);
        transactionLog.grantReadFilesLog(grantee);
        transactionLog.grantReadPartitionsLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }

    public void grantReadWriteAllFilesAndPartitions(IGrantable grantee) {
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWriteFilesLog(grantee);
        transactionLog.grantReadWritePartitionsLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }

    public void grantReadActiveFilesReadWritePartitions(IGrantable grantee) {
        dynamo.grantReadActiveFileMetadata(grantee);
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadFilesLog(grantee);
        transactionLog.grantReadWritePartitionsLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }

    public void grantReadPartitionsReadWriteActiveFiles(IGrantable grantee) {
        dynamo.grantReadPartitionMetadata(grantee);
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadPartitionsLog(grantee);
        transactionLog.grantReadWriteFilesLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }

    public void grantReadPartitions(IGrantable grantee) {
        dynamo.grantReadPartitionMetadata(grantee);
        s3.grantRead(grantee);
        transactionLog.grantReadPartitionsLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }

    public void grantReadWriteActiveAndReadyForGCFiles(IGrantable grantee) {
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWriteFilesLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }

    public void grantReadWriteReadyForGCFiles(IGrantable grantee) {
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWriteFilesLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }

    public void grantReadWritePartitions(IGrantable grantee) {
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
        transactionLog.grantReadWritePartitionsLog(grantee);
        transactionLog.grantReadLatestSnapshots(grantee);
    }
}
