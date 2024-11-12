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
package sleeper.core.statestore.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

/**
 * Handles uploading commit requests to S3 if they are too big to fit in an SQS message.
 */
public class StateStoreCommitRequestInS3Uploader {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitRequestInS3Uploader.class);
    public static final int MAX_JSON_LENGTH = 262144;

    private final InstanceProperties instanceProperties;
    private final Client client;
    private final Supplier<String> filenameSupplier;
    private final int maxJsonLength;
    private final StateStoreCommitRequestInS3SerDe serDe = new StateStoreCommitRequestInS3SerDe();

    public StateStoreCommitRequestInS3Uploader(InstanceProperties instanceProperties, Client client) {
        this(instanceProperties, client, MAX_JSON_LENGTH, () -> UUID.randomUUID().toString());
    }

    public StateStoreCommitRequestInS3Uploader(InstanceProperties instanceProperties, Client client, int maxJsonLength, Supplier<String> filenameSupplier) {
        this.instanceProperties = instanceProperties;
        this.client = client;
        this.maxJsonLength = maxJsonLength;
        this.filenameSupplier = filenameSupplier;
    }

    /**
     * Checks whether a state store commit request JSON will fit in an SQS message. If not, uploads it to S3 and creates
     * a new commit request referencing the original JSON in S3.
     *
     * @param  tableId           the Sleeper table ID
     * @param  commitRequestJson the commit request JSON
     * @return                   the commit request if it fits in an SQS message, or a new commit request referencing S3
     */
    public String uploadAndWrapIfTooBig(String tableId, String commitRequestJson) {
        // Store in S3 if the request will not fit in an SQS message
        if (commitRequestJson.length() > maxJsonLength) {
            String s3Key = StateStoreCommitRequestInS3.createFileS3Key(tableId, filenameSupplier.get());
            client.putObject(instanceProperties.get(DATA_BUCKET), s3Key, commitRequestJson);
            LOGGER.info("Request was too big for an SQS message. Will submit a reference to file in data bucket: {}", s3Key);
            return serDe.toJson(new StateStoreCommitRequestInS3(s3Key));
        } else {
            return commitRequestJson;
        }
    }

    /**
     * A client to upload an object to S3.
     */
    public interface Client {

        /**
         * Uploads an object to an S3 bucket.
         *
         * @param bucketName the bucket name
         * @param key        the key to upload to
         * @param content    the content of the file to upload
         */
        void putObject(String bucketName, String key, String content);
    }
}
