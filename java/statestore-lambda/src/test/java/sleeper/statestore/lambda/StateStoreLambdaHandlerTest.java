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
package sleeper.statestore.lambda;

import org.junit.jupiter.api.Test;

import sleeper.core.deploy.LambdaHandler;
import sleeper.statestore.lambda.committer.CompactionCommitBatcherLambda;
import sleeper.statestore.lambda.committer.StateStoreCommitterLambda;
import sleeper.statestore.lambda.snapshot.TransactionLogSnapshotCreationLambda;
import sleeper.statestore.lambda.snapshot.TransactionLogSnapshotCreationTriggerLambda;
import sleeper.statestore.lambda.snapshot.TransactionLogSnapshotDeletionLambda;
import sleeper.statestore.lambda.snapshot.TransactionLogSnapshotDeletionTriggerLambda;
import sleeper.statestore.lambda.transaction.TransactionLogFollowerLambda;
import sleeper.statestore.lambda.transaction.TransactionLogTransactionDeletionLambda;
import sleeper.statestore.lambda.transaction.TransactionLogTransactionDeletionTriggerLambda;

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreLambdaHandlerTest {

    @Test
    void shouldMatchCommitterLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.STATESTORE_COMMITTER.getHandler())
                .isEqualTo(StateStoreCommitterLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchCompactionBatcherLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.COMPACTION_COMMIT_BATCHER.getHandler())
                .isEqualTo(CompactionCommitBatcherLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchSnapshotCreationTriggerLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.SNAPSHOT_CREATION_TRIGGER.getHandler())
                .isEqualTo(TransactionLogSnapshotCreationTriggerLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchSnapshotCreationLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.SNAPSHOT_CREATION.getHandler())
                .isEqualTo(TransactionLogSnapshotCreationLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchSnapshotDeletionTriggerLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.SNAPSHOT_DELETION_TRIGGER.getHandler())
                .isEqualTo(TransactionLogSnapshotDeletionTriggerLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchSnapshotDeletionLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.SNAPSHOT_DELETION.getHandler())
                .isEqualTo(TransactionLogSnapshotDeletionLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchTransactionDeletionTriggerLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.TRANSACTION_DELETION_TRIGGER.getHandler())
                .isEqualTo(TransactionLogTransactionDeletionTriggerLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchTransactionDeletionLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.TRANSACTION_DELETION.getHandler())
                .isEqualTo(TransactionLogTransactionDeletionLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchTransactionFollowerLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.TRANSACTION_FOLLOWER.getHandler())
                .isEqualTo(TransactionLogFollowerLambda.class.getName() + "::handleRequest");
    }

}
