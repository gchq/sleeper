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
package sleeper.cdk.stack;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.IQueue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class ManagedPoliciesStack extends NestedStack {

    private final InstanceProperties instanceProperties;
    private final ManagedPolicy directIngestPolicy;
    private final ManagedPolicy ingestByQueuePolicy;
    private final ManagedPolicy queryPolicy;
    private final ManagedPolicy editTablesPolicy;
    private final ManagedPolicy reportingPolicy;
    private final ManagedPolicy readIngestSourcesPolicy;
    private final ManagedPolicy clearInstancePolicy;
    private ManagedPolicy purgeQueuesPolicy;
    private ManagedPolicy invokeCompactionPolicy;
    private ManagedPolicy invokeSchedulesPolicy;

    public ManagedPoliciesStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);
        this.instanceProperties = instanceProperties;

        directIngestPolicy = createManagedPolicy("DirectIngestPolicy");
        ingestByQueuePolicy = createManagedPolicy("IngestByQueuePolicy");

        queryPolicy = createManagedPolicy("QueryPolicy");
        editTablesPolicy = createManagedPolicy("EditTablesPolicy");
        reportingPolicy = createManagedPolicy("ReportingPolicy");
        clearInstancePolicy = createManagedPolicy("ClearInstancePolicy");

        List<IBucket> sourceBuckets = addIngestSourceBucketReferences(this, instanceProperties);
        if (sourceBuckets.isEmpty()) { // CDK doesn't allow a managed policy without any grants
            readIngestSourcesPolicy = null;
        } else {
            readIngestSourcesPolicy = createManagedPolicy("ReadIngestSourcesPolicy");
            sourceBuckets.forEach(bucket -> bucket.grantRead(readIngestSourcesPolicy));
        }
    }

    public ManagedPolicy getDirectIngestPolicyForGrants() {
        return directIngestPolicy;
    }

    public ManagedPolicy getIngestByQueuePolicyForGrants() {
        return ingestByQueuePolicy;
    }

    public ManagedPolicy getQueryPolicyForGrants() {
        return queryPolicy;
    }

    public ManagedPolicy getEditTablesPolicyForGrants() {
        return editTablesPolicy;
    }

    public ManagedPolicy getReportingPolicyForGrants() {
        return reportingPolicy;
    }

    public ManagedPolicy getClearInstancePolicyForGrants() {
        return clearInstancePolicy;
    }

    public ManagedPolicy getPurgeQueuesPolicyForGrants() {
        // Avoid creating empty policy when we're not deploying any queues
        if (purgeQueuesPolicy == null) {
            purgeQueuesPolicy = createManagedPolicy("PurgeQueuesPolicy");
        }
        return purgeQueuesPolicy;
    }

    public ManagedPolicy getInvokeCompactionPolicyForGrants() {
        // Avoid creating empty policy when we're not deploying compaction stack
        if (invokeCompactionPolicy == null) {
            invokeCompactionPolicy = createManagedPolicy("InvokeCompactionPolicy");
        }
        return invokeCompactionPolicy;
    }

    public void grantInvokeScheduled(IFunction function) {
        // Avoid creating empty policy when we're not deploying any scheduled rules
        if (invokeSchedulesPolicy == null) {
            invokeSchedulesPolicy = createManagedPolicy("InvokeSchedulesPolicy");
        }
        Utils.grantInvokeOnPolicy(function, invokeSchedulesPolicy);
    }

    public void grantInvokeScheduled(IFunction function, IQueue invokeQueue) {
        grantInvokeScheduled(function);
        invokeQueue.grantSendMessages(invokeSchedulesPolicy);
    }

    public void grantReadIngestSources(IRole role) {
        if (readIngestSourcesPolicy != null) {
            readIngestSourcesPolicy.attachToRole(role);
        }
    }

    Stream<ManagedPolicy> instanceAdminPolicies() {
        return Stream.of(
                directIngestPolicy, ingestByQueuePolicy, queryPolicy,
                editTablesPolicy, reportingPolicy, clearInstancePolicy,
                purgeQueuesPolicy, invokeCompactionPolicy, invokeSchedulesPolicy)
                .filter(policy -> policy != null);
    }

    private ManagedPolicy createManagedPolicy(String id) {
        return ManagedPolicy.Builder.create(this, id)
                .managedPolicyName(String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties), id))
                .build();
    }

    private static List<IBucket> addIngestSourceBucketReferences(Construct scope, InstanceProperties instanceProperties) {
        AtomicInteger index = new AtomicInteger(1);
        return instanceProperties.getList(INGEST_SOURCE_BUCKET).stream()
                .filter(not(String::isBlank))
                .map(bucketName -> Bucket.fromBucketName(scope, "SourceBucket" + index.getAndIncrement(), bucketName))
                .collect(Collectors.toList());
    }
}
