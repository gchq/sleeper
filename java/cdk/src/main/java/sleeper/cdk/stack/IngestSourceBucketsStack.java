/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.cdk.stack;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class IngestSourceBucketsStack extends NestedStack {
    private final List<IBucket> sourceBuckets;

    public static GrantBuckets create(Construct scope, String id, InstanceProperties instanceProperties) {
        if (instanceProperties.getList(INGEST_SOURCE_BUCKET).stream()
                .filter(not(String::isBlank)).count() > 0) {
            return new GrantBuckets(
                    new IngestSourceBucketsStack(scope, id, instanceProperties).sourceBuckets);
        } else {
            return new GrantBuckets(List.of());
        }
    }

    private IngestSourceBucketsStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);

        sourceBuckets = addIngestSourceBucketReferences(this, instanceProperties);
    }

    private static List<IBucket> addIngestSourceBucketReferences(Construct scope, InstanceProperties instanceProperties) {
        AtomicInteger index = new AtomicInteger(1);
        return instanceProperties.getList(INGEST_SOURCE_BUCKET).stream()
                .filter(not(String::isBlank))
                .map(bucketName -> Bucket.fromBucketName(scope, "SourceBucket" + index.getAndIncrement(), bucketName))
                .collect(Collectors.toList());
    }

    static class GrantBuckets {
        List<IBucket> sourceBuckets;

        GrantBuckets(List<IBucket> sourceBuckets) {
            this.sourceBuckets = sourceBuckets;
        }

        void grantReadIngestSources(IGrantable grantee) {
            sourceBuckets.forEach(bucket -> bucket.grantRead(grantee));
        }
    }
}
