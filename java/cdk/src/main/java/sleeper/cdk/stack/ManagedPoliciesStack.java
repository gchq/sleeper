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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.iam.AccountRootPrincipal;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.IQueue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ADMIN_ROLE_ARN;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_ROLE;

public class ManagedPoliciesStack extends NestedStack {

    private final ManagedPolicy ingestPolicy;
    private final ManagedPolicy queryPolicy;
    private final ManagedPolicy editTablesPolicy;
    private final ManagedPolicy reportingPolicy;
    private final ManagedPolicy invokeSchedulesPolicy;
    private final ManagedPolicy invokeCompactionPolicy;
    private final ManagedPolicy purgeQueuesPolicy;
    private final ManagedPolicy readIngestSourcesPolicy;

    public ManagedPoliciesStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);

        ingestPolicy = new ManagedPolicy(this, "IngestPolicy");
        addRoleReferences(this, instanceProperties, INGEST_SOURCE_ROLE, "IngestSourceRole")
                .forEach(ingestPolicy::attachToRole);

        queryPolicy = new ManagedPolicy(this, "QueryPolicy");
        editTablesPolicy = new ManagedPolicy(this, "EditTablesPolicy");
        reportingPolicy = new ManagedPolicy(this, "ReportingPolicy");
        invokeSchedulesPolicy = new ManagedPolicy(this, "InvokeSchedulesPolicy");
        invokeCompactionPolicy = new ManagedPolicy(this, "InvokeCompactionPolicy");
        purgeQueuesPolicy = new ManagedPolicy(this, "PurgeQueuesPolicy");

        List<IBucket> sourceBuckets = addIngestSourceBucketReferences(this, instanceProperties);
        if (sourceBuckets.isEmpty()) { // CDK doesn't allow a managed policy without any grants
            readIngestSourcesPolicy = null;
        } else {
            readIngestSourcesPolicy = new ManagedPolicy(this, "ReadIngestSourcesPolicy");
            sourceBuckets.forEach(bucket -> bucket.grantRead(readIngestSourcesPolicy));
        }

        reportingPolicy.addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("cloudwatch:GetMetricData"))
                .resources(List.of("*"))
                .build());

        Role adminRole = Role.Builder.create(this, "AdminRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-admin-" + instanceProperties.get(ID).toLowerCase(Locale.ROOT))
                .build();
        Stream.of(ingestPolicy, queryPolicy, editTablesPolicy, reportingPolicy, invokeSchedulesPolicy, invokeCompactionPolicy, purgeQueuesPolicy)
                .forEach(policy -> policy.attachToRole(adminRole));
        instanceProperties.set(ADMIN_ROLE_ARN, adminRole.getRoleArn());
    }

    public ManagedPolicy getIngestPolicy() {
        return ingestPolicy;
    }

    public ManagedPolicy getQueryPolicy() {
        return queryPolicy;
    }

    public ManagedPolicy getEditTablesPolicy() {
        return editTablesPolicy;
    }

    public ManagedPolicy getReportingPolicy() {
        return reportingPolicy;
    }

    public ManagedPolicy getInvokeCompactionPolicy() {
        return invokeCompactionPolicy;
    }

    public ManagedPolicy getPurgeQueuesPolicy() {
        return purgeQueuesPolicy;
    }

    public void grantInvokeScheduled(IFunction function) {
        Utils.grantInvokeOnPolicy(function, invokeSchedulesPolicy);
    }

    public void grantInvokeScheduled(IFunction function, IQueue tableBatchQueue) {
        grantInvokeScheduled(function);
        tableBatchQueue.grantSendMessages(invokeSchedulesPolicy);
    }

    // The Lambda IFunction.getRole method is annotated as nullable, even though it will never return null in practice.
    // This means SpotBugs complains if we pass that role into attachToRole.
    // The role parameter is marked as nullable to convince SpotBugs that it's fine to pass it into this method,
    // even though attachToRole really requires the role to be non-null.
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public void grantReadIngestSources(@Nullable IRole role) {
        if (readIngestSourcesPolicy != null) {
            readIngestSourcesPolicy.attachToRole(Objects.requireNonNull(role));
        }
    }

    // WARNING: When assigning grants to these roles, the ID of the role reference is incorrectly used as the name of
    //          the IAM policy. This means the resulting ID must be unique within your AWS account. This is a bug in
    //          the CDK.
    //          The result is that in order to have more than one instance of Sleeper deployed to the same AWS account,
    //          we include the instance ID in the CDK logical IDs for the ingest source roles.
    //          This should not be necessary with a managed policy, but when you use the CDK's grantX methods directly,
    //          it adds separate policies against the roles. That may still be desirable in the future.
    private static List<IRole> addRoleReferences(
            Construct scope, InstanceProperties instanceProperties, InstanceProperty property, String roleId) {
        AtomicInteger index = new AtomicInteger(1);
        return instanceProperties.getList(property).stream()
                .filter(not(String::isBlank))
                .map(name -> Role.fromRoleName(scope, roleReferenceId(instanceProperties, roleId, index), name))
                .collect(Collectors.toUnmodifiableList());
    }

    private static List<IBucket> addIngestSourceBucketReferences(Construct scope, InstanceProperties instanceProperties) {
        AtomicInteger index = new AtomicInteger(1);
        return instanceProperties.getList(INGEST_SOURCE_BUCKET).stream()
                .filter(not(String::isBlank))
                .map(bucketName -> Bucket.fromBucketName(scope, "SourceBucket" + index.getAndIncrement(), bucketName))
                .collect(Collectors.toList());
    }

    private static String roleReferenceId(InstanceProperties instanceProperties, String roleId, AtomicInteger index) {
        return Utils.truncateTo64Characters(String.join("-",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT),
                String.valueOf(index.getAndIncrement()), roleId));
    }
}
