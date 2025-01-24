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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.core.util.LoggedDuration;

import java.time.Instant;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

/**
 * Stores the body of transactions in an S3 bucket.
 */
public class S3TransactionBodyStore implements TransactionBodyStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3TransactionBodyStore.class);
    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;
    private final TransactionSerDeProvider serDeProvider;

    public S3TransactionBodyStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonS3 s3Client) {
        this(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties));
    }

    public S3TransactionBodyStore(InstanceProperties instanceProperties, AmazonS3 s3Client, TransactionSerDeProvider serDeProvider) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        this.serDeProvider = serDeProvider;
    }

    @Override
    public void store(String key, String tableId, StateStoreTransaction<?> transaction) {
        store(key, serDeProvider.getByTableId(tableId).toJson(transaction));
    }

    /**
     * Stores a transaction body that's already been serialised as a string.
     *
     * @param key  the object key in the data bucket to store the file in
     * @param body the transaction body
     */
    public void store(String key, String body) {
        Instant startTime = Instant.now();
        s3Client.putObject(instanceProperties.get(DATA_BUCKET), key, body);
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
