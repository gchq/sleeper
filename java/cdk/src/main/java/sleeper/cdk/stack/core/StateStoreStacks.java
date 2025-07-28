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

package sleeper.cdk.stack.core;

import software.amazon.awscdk.services.iam.IGrantable;

import static sleeper.cdk.stack.core.StateStoreGrants.readAllFilesAndPartitions;
import static sleeper.cdk.stack.core.StateStoreGrants.readFileReferencesAndPartitions;
import static sleeper.cdk.stack.core.StateStoreGrants.readFileReferencesReadWritePartitions;
import static sleeper.cdk.stack.core.StateStoreGrants.readPartitions;
import static sleeper.cdk.stack.core.StateStoreGrants.readPartitionsReadWriteFileReferences;
import static sleeper.cdk.stack.core.StateStoreGrants.readWriteAllFilesAndPartitions;
import static sleeper.cdk.stack.core.StateStoreGrants.readWriteFileReferencesAndUnreferenced;
import static sleeper.cdk.stack.core.StateStoreGrants.readWritePartitions;
import static sleeper.cdk.stack.core.StateStoreGrants.readWriteUnreferencedFiles;

public final class StateStoreStacks {

    private final TransactionLogStateStoreStack transactionLog;

    public StateStoreStacks(
            TransactionLogStateStoreStack transactionLog,
            ManagedPoliciesStack policiesStack) {
        this.transactionLog = transactionLog;
        grantAccess(readPartitionsReadWriteFileReferences(), policiesStack.getDirectIngestPolicyForGrants());
        grantAccess(readFileReferencesAndPartitions(), policiesStack.getQueryPolicyForGrants());
        grantAccess(readAllFilesAndPartitions(), policiesStack.getReportingPolicyForGrants());
        transactionLog.grantReadAllSnapshotsTable(policiesStack.getReportingPolicyForGrants());
        grantAccess(readWriteAllFilesAndPartitions(), policiesStack.getClearInstancePolicyForGrants());
        transactionLog.grantClearSnapshots(policiesStack.getClearInstancePolicyForGrants());
        grantAccess(readWritePartitions(), policiesStack.getEditTablesPolicyForGrants());
    }

    public void grantReadFileReferencesAndPartitions(IGrantable grantee) {
        grantAccess(readFileReferencesAndPartitions(), grantee);
    }

    public void grantReadWriteAllFilesAndPartitions(IGrantable grantee) {
        grantAccess(readWriteAllFilesAndPartitions(), grantee);
    }

    public void grantReadFileReferencesReadWritePartitions(IGrantable grantee) {
        grantAccess(readFileReferencesReadWritePartitions(), grantee);
    }

    public void grantReadPartitionsReadWriteFileReferences(IGrantable grantee) {
        grantAccess(readPartitionsReadWriteFileReferences(), grantee);
    }

    public void grantReadPartitions(IGrantable grantee) {
        grantAccess(readPartitions(), grantee);
    }

    public void grantReadWriteFileReferencesAndUnreferenced(IGrantable grantee) {
        grantAccess(readWriteFileReferencesAndUnreferenced(), grantee);
    }

    public void grantReadWriteUnreferencedFiles(IGrantable grantee) {
        grantAccess(readWriteUnreferencedFiles(), grantee);
    }

    public void grantReadWritePartitions(IGrantable grantee) {
        grantAccess(readWritePartitions(), grantee);
    }

    public void grantAccess(StateStoreGrants grants, IGrantable grantee) {
        transactionLog.grantAccess(grants, grantee);
    }

    public void grantReadAllSnapshotsTable(IGrantable grantee) {
        transactionLog.grantReadAllSnapshotsTable(grantee);
    }
}
