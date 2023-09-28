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

package sleeper.cdk.stack;

import software.amazon.awscdk.services.iam.IGrantable;

public class StateStoreStacks {

    private final DynamoDBStateStoreStack dynamo;
    private final S3StateStoreStack s3;

    public StateStoreStacks(DynamoDBStateStoreStack dynamo, S3StateStoreStack s3) {
        this.dynamo = dynamo;
        this.s3 = s3;
    }

    public void grantReadActiveFilesAndPartitions(IGrantable grantee) {
        dynamo.grantReadActiveFileMetadata(grantee);
        dynamo.grantReadPartitionMetadata(grantee);
        s3.grantRead(grantee);
    }

    public void grantReadWriteActiveFilesAndPartitions(IGrantable grantee) {
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
    }

    public void grantReadActiveFilesReadWritePartitions(IGrantable grantee) {
        dynamo.grantReadActiveFileMetadata(grantee);
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
    }

    public void grantReadPartitionsReadWriteActiveFiles(IGrantable grantee) {
        dynamo.grantReadPartitionMetadata(grantee);
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        s3.grantReadWrite(grantee);
    }

    public void grantReadPartitions(IGrantable grantee) {
        dynamo.grantReadPartitionMetadata(grantee);
        s3.grantRead(grantee);
    }

    public void grantReadWriteActiveAndReadyForGCFiles(IGrantable grantee) {
        dynamo.grantReadWriteActiveFileMetadata(grantee);
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        s3.grantReadWrite(grantee);
    }

    public void grantReadWriteReadyForGCFiles(IGrantable grantee) {
        dynamo.grantReadWriteReadyForGCFileMetadata(grantee);
        s3.grantReadWrite(grantee);
    }

    public void grantReadWritePartitions(IGrantable grantee) {
        dynamo.grantReadWritePartitionMetadata(grantee);
        s3.grantReadWrite(grantee);
    }
}
