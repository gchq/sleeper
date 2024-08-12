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
package sleeper.systemtest.dsl.statestore;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

public class SystemTestStateStoreFakeCommits {

    private final SystemTestInstanceContext instance;
    private final StateStoreCommitterDriver driver;
    private final AtomicInteger commitsSent = new AtomicInteger();

    public SystemTestStateStoreFakeCommits(SystemTestContext context) {
        instance = context.instance();
        driver = context.instance().adminDrivers().stateStoreCommitter(context);
    }

    public SystemTestStateStoreFakeCommits sendBatched(Function<StateStoreCommitMessageFactory, Stream<StateStoreCommitMessage>> buildCommits) {
        send(buildCommits.apply(messageFactory()));
        return this;
    }

    public SystemTestStateStoreFakeCommits send(Function<StateStoreCommitMessageFactory, StateStoreCommitMessage> buildCommit) {
        send(Stream.of(buildCommit.apply(messageFactory())));
        return this;
    }

    private void send(Stream<StateStoreCommitMessage> messages) {
        driver.sendCommitMessages(messages
                .peek(message -> commitsSent.incrementAndGet()));
    }

    private StateStoreCommitMessageFactory messageFactory() {
        return new StateStoreCommitMessageFactory(instance.getTableStatus().getTableUniqueId());
    }
}
