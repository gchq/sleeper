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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyPointer;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.core.util.LoggedDuration;

import java.time.Instant;

/**
 * Stores the body of transactions in an S3 bucket.
 */
public class S3TransactionBodyStore implements TransactionBodyStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3TransactionBodyStore.class);
    private final AmazonS3 s3Client;
    private final TransactionSerDe serDe;

    public S3TransactionBodyStore(TableProperties tableProperties, AmazonS3 s3Client) {
        this.s3Client = s3Client;
        this.serDe = new TransactionSerDe(tableProperties.getSchema());
    }

    @Override
    public void store(TransactionBodyPointer pointer, StateStoreTransaction<?> transaction) {
        //TODO expand functionality here
    }

    /**
     * Stores a transaction body that's already been serialised as a string.
     *
     * @param bucketName the name of the bucket
     * @param key        the object key
     * @param body       the transaction body
     */
    public void store(String bucketName, String key, String body) {
        Instant startTime = Instant.now();
        s3Client.putObject(bucketName, key, body);
        LOGGER.info("Saved to S3 in {}", LoggedDuration.withShortOutput(startTime, Instant.now()));
    }

    @Override
    public <T extends StateStoreTransaction<?>> T getBody(TransactionBodyPointer pointer, TransactionType transactionType) {
        LOGGER.debug("Reading large {} transaction from data bucket at {}", transactionType, pointer.getKey());
        String body = s3Client.getObjectAsString(pointer.getBucketName(), pointer.getKey());
        return (T) serDe.toTransaction(transactionType, body);
    }

    public void delete(TransactionBodyPointer pointer) {
        s3Client.deleteObject(pointer.getBucketName(), pointer.getKey());
    }
}
