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
package sleeper.core.statestore.transactionlog.log;

/**
 * Failure adding a transaction when one already exists with that number. This means there is at least one transaction
 * in the log that has not been read yet, as we will have tried the number immediately after the last one we read.
 * <p>
 * This will usually result in a retry. The current state will be updated with any unread transactions, the new
 * transaction will be revalidated against the new state, and then it will be added with a new transaction number.
 */
public class DuplicateTransactionNumberException extends Exception {

    public DuplicateTransactionNumberException(long attemptedTransactionNumber) {
        this(attemptedTransactionNumber, null);
    }

    public DuplicateTransactionNumberException(long attemptedTransactionNumber, Throwable cause) {
        super("Unread transaction found. Adding transaction number " + attemptedTransactionNumber + ", " +
                "but it already exists.", cause);
    }

}
