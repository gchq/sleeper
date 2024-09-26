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

package sleeper.systemtest.drivers.query;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.ClearQueryResultsDriver;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class S3ResultsDriver implements ClearQueryResultsDriver {
    private final SystemTestInstanceContext instance;
    private final AmazonS3 s3;

    public S3ResultsDriver(SystemTestInstanceContext instance, AmazonS3 s3) {
        this.instance = instance;
        this.s3 = s3;
    }

    public void deleteAllQueryResults() {
        s3.listObjects(instance.getInstanceProperties().get(QUERY_RESULTS_BUCKET)).getObjectSummaries()
                .forEach(summary -> s3.deleteObject(summary.getBucketName(), summary.getKey()));
    }
}
