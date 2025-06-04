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

package sleeper.systemtest.drivers.query;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.ClearQueryResultsDriver;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class S3ResultsDriver implements ClearQueryResultsDriver {
    private final SystemTestInstanceContext instance;
    private final S3Client s3Client;

    public S3ResultsDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.s3Client = clients.getS3();
    }

    public void deleteAllQueryResults() {
        String bucketName = instance.getInstanceProperties().get(QUERY_RESULTS_BUCKET);
        s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build())
                .contents().forEach(object -> s3Client.deleteObject(
                        DeleteObjectRequest.builder().bucket(bucketName).key(object.key()).build()));
    }
}
