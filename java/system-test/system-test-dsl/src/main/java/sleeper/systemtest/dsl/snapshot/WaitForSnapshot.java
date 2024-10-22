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
package sleeper.systemtest.dsl.snapshot;

import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.Optional;

public class WaitForSnapshot {

    private final SystemTestInstanceContext instance;
    private final SnapshotsDriver driver;

    public WaitForSnapshot(SystemTestInstanceContext instance, SnapshotsDriver driver) {
        this.instance = instance;
        this.driver = driver;
    }

    public TransactionLogSnapshot waitForFilesSnapshot(PollWithRetries poll) throws InterruptedException {
        return poll.queryUntil("files snapshot is present", this::loadLatestFilesSnapshot, Optional::isPresent)
                .orElseThrow();
    }

    public TransactionLogSnapshot waitForPartitionsSnapshot(PollWithRetries poll) throws InterruptedException {
        return poll.queryUntil("partitions snapshot is present", this::loadLatestPartitionsSnapshot, Optional::isPresent)
                .orElseThrow();
    }

    private Optional<TransactionLogSnapshot> loadLatestFilesSnapshot() {
        return driver.loadLatestFilesSnapshot(instance.getInstanceProperties(), instance.getTableProperties());
    }

    private Optional<TransactionLogSnapshot> loadLatestPartitionsSnapshot() {
        return driver.loadLatestPartitionsSnapshot(instance.getInstanceProperties(), instance.getTableProperties());
    }

}
