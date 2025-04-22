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
package sleeper.statestorev2.transactionlog;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.transactionlog.log.StoreTransactionBodyResult;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.util.LoggedDuration;

import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

/**
 * Stores the body of transactions in an S3 bucket.
 */
public class S3TransactionBodyStore implements TransactionBodyStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3TransactionBodyStore.class);

    /**
     * Minimum length of a JSON string that will be written to S3.
     * <p>
     * A transaction will be held in a DynamoDB item for the log entry with {@link DynamoDBTransactionLogStore}. A
     * transaction will be held in an SQS message when sending an asynchronous commit.
     * <p>
     * Max DynamoDB item size is 400KiB. Space is needed for the rest of the item. DynamoDB uses UTF-8 encoding for
     * strings.
     * <p>
     * Max size of an SQS message is 256KiB. Space is needed for the request wrapper.
     * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
     */
    public static final int DEFAULT_JSON_LENGTH_TO_STORE = 1024 * 200;

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;
    private final TransactionSerDeProvider serDeProvider;
    private final int jsonLengthToStore;

    public S3TransactionBodyStore(InstanceProperties instanceProperties, AmazonS3 s3Client, TransactionSerDeProvider serDeProvider) {
        this(instanceProperties, s3Client, serDeProvider, DEFAULT_JSON_LENGTH_TO_STORE);
    }

    public S3TransactionBodyStore(InstanceProperties instanceProperties, AmazonS3 s3Client, TransactionSerDeProvider serDeProvider, int jsonLengthToStore) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        this.serDeProvider = serDeProvider;
        this.jsonLengthToStore = jsonLengthToStore;
    }

    @Override
    public void store(String key, String tableId, StateStoreTransaction<?> transaction) {
        store(key, transaction, serDeProvider.getByTableId(tableId).toJson(transaction));
    }

    @Override
    public StoreTransactionBodyResult storeIfTooBig(String tableId, StateStoreTransaction<?> transaction) {
        String json = serDeProvider.getByTableId(tableId).toJson(transaction);
        if (json.length() < jsonLengthToStore) {
            return StoreTransactionBodyResult.notStored(json);
        } else {
            String key = TransactionBodyStore.createObjectKey(tableId);
            store(key, transaction, json);
            return StoreTransactionBodyResult.stored(key);
        }
    }

    private void store(String key, StateStoreTransaction<?> transaction, String body) {
        TransactionType transactionType = TransactionType.getType(transaction);
        LOGGER.debug("Uploading large transaction of type {} to S3 at: {}", transactionType, key);
        Instant startTime = Instant.now();
        s3Client.putObject(instanceProperties.get(DATA_BUCKET), key, body);
        LOGGER.debug("Uploaded large transaction of type {} to S3 at: {}", transactionType, key);
        LOGGER.info("Saved to S3 in {}", LoggedDuration.withShortOutput(startTime, Instant.now()));
    }

    @Override
    public <T extends StateStoreTransaction<?>> T getBody(String key, String tableId, TransactionType transactionType) {
        LOGGER.debug("Reading large {} transaction from data bucket at {}", transactionType, key);
        String body = s3Client.getObjectAsString(instanceProperties.get(DATA_BUCKET), key);
        return (T) serDeProvider.getByTableId(tableId).toTransaction(transactionType, body);
    }

    /**
     * Deletes a transaction body from the bucket.
     *
     * @param key the S3 object key
     */
    public void delete(String key) {
        s3Client.deleteObject(instanceProperties.get(DATA_BUCKET), key);
    }
}
