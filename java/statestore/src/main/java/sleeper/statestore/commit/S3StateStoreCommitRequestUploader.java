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
package sleeper.statestore.commit;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDeProvider;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Uploads transactions to S3 if they are larger than a certain limit.
 */
public class S3StateStoreCommitRequestUploader {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3StateStoreCommitRequestUploader.class);

    private final S3TransactionBodyStore transactionBodyStore;
    private final TransactionSerDeProvider transactionSerDeProvider;
    private final StateStoreCommitRequestSerDe requestSerDe;
    private final Supplier<Instant> timeSupplier;
    private final Supplier<String> idSupplier;

    public S3StateStoreCommitRequestUploader(
            InstanceProperties instanceProperties, TransactionSerDeProvider transactionSerDeProvider, AmazonS3 s3Client,
            Supplier<Instant> timeSupplier, Supplier<String> idSupplier) {
        this.transactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, transactionSerDeProvider);
        this.transactionSerDeProvider = transactionSerDeProvider;
        this.requestSerDe = new StateStoreCommitRequestSerDe(transactionSerDeProvider);
        this.timeSupplier = timeSupplier;
        this.idSupplier = idSupplier;
    }

    /**
     * Serialises a commit request to a string, uploading the transaction to S3 if it is too big.
     *
     * @param  maxTransactionBytes the maximum size of a transaction in bytes
     * @param  request             the request
     * @return                     the string
     */
    public String serialiseAndUploadIfTooBig(int maxTransactionBytes, StateStoreCommitRequest request) {
        return requestSerDe.toJson(request.getTransactionIfHeld()
                .flatMap(transaction -> uploadTransactionIfTooBig(maxTransactionBytes, request, transaction))
                .orElse(request));
    }

    private Optional<StateStoreCommitRequest> uploadTransactionIfTooBig(
            int maxTransactionBytes, StateStoreCommitRequest request, StateStoreTransaction<?> transaction) {
        TransactionSerDe transactionSerDe = transactionSerDeProvider.getByTableId(request.getTableId());
        String json = transactionSerDe.toJson(transaction);
        if (json.length() < maxTransactionBytes) {
            return Optional.empty();
        } else {
            String key = TransactionBodyStore.createObjectKey(request.getTableId(), timeSupplier.get(), idSupplier.get());
            LOGGER.debug("Uploading large transaction of type {} to S3 at: {}", request.getTransactionType(), key);
            transactionBodyStore.store(key, json);
            LOGGER.debug("Uploaded large transaction of type {} to S3 at: {}", request.getTransactionType(), key);
            return Optional.of(StateStoreCommitRequest.create(request.getTableId(), key, request.getTransactionType()));
        }
    }

}
