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
package sleeper.statestore.lambda.transaction;

import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;

/**
 * A handle to a transaction log entry processed in a transaction log follower.
 *
 * @param tableId        the Sleeper table ID
 * @param itemIdentifier the sequence number of the stream record this was derived from
 * @param entry          the transaction log entry
 */
public record TransactionLogEntryHandle(String tableId, String itemIdentifier, TransactionLogEntry entry) {

}
