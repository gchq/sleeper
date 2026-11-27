/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.runner;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.query.core.model.QueryJsonStore;
import sleeper.query.core.model.QueryMessageReader;
import sleeper.query.core.model.QueryMessageUploader;
import sleeper.query.core.model.QuerySerDe;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

/**
 * Stores a query that when serialised to JSON is too large for an SQS message in the query results bucket in S3.
 */
public class S3QueryJsonStore implements QueryJsonStore {

    private final S3Client s3Client;
    private final String bucketName;

    public S3QueryJsonStore(InstanceProperties instanceProperties, S3Client s3Client) {
        this(s3Client, instanceProperties.get(QUERY_RESULTS_BUCKET));
    }

    public S3QueryJsonStore(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    /**
     * Creates an uploader for query messages which will upload large queries to S3.
     *
     * @param  instanceProperties      the instance properties
     * @param  tablePropertiesProvider the table properties provider
     * @param  s3Client                the S3 client
     * @return                         the uploader
     */
    public static QueryMessageUploader createUploader(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, S3Client s3Client) {
        return createUploader(instanceProperties, new QuerySerDe(tablePropertiesProvider), s3Client);
    }

    /**
     * Creates an uploader for query messages which will upload large queries to S3.
     *
     * @param  instanceProperties the instance properties
     * @param  querySerDe         the query serialiser
     * @param  s3Client           the S3 client
     * @return                    the uploader
     */
    public static QueryMessageUploader createUploader(
            InstanceProperties instanceProperties, QuerySerDe querySerDe, S3Client s3Client) {
        return new QueryMessageUploader(new S3QueryJsonStore(instanceProperties, s3Client), querySerDe);
    }

    /**
     * Creates a reader for query messages which will load large queries from S3.
     *
     * @param  instanceProperties      the instance properties
     * @param  tablePropertiesProvider the table properties provider
     * @param  s3Client                the S3 client
     * @return                         the reader
     */
    public static QueryMessageReader createReader(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, S3Client s3Client) {
        return createReader(instanceProperties, new QuerySerDe(tablePropertiesProvider), s3Client);
    }

    /**
     * Creates a reader for query messages which will load large queries from S3.
     *
     * @param  instanceProperties the instance properties
     * @param  querySerDe         the query serialiser
     * @param  s3Client           the S3 client
     * @return                    the reader
     */
    public static QueryMessageReader createReader(
            InstanceProperties instanceProperties, QuerySerDe querySerDe, S3Client s3Client) {
        return new QueryMessageReader(new S3QueryJsonStore(instanceProperties, s3Client), querySerDe);
    }

    @Override
    public void store(String key, String json) {
        s3Client.putObject(request -> request.bucket(bucketName).key(key), RequestBody.fromString(json));
    }

    @Override
    public String load(String key) {
        return s3Client.getObject(request -> request.bucket(bucketName).key(key), ResponseTransformer.toBytes())
                .asUtf8String();
    }
}
