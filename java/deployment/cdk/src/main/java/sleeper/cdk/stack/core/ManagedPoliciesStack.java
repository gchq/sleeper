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
package sleeper.cdk.stack.core;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.iam.AccountRootPrincipal;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.IQueue;
import software.constructs.Construct;

import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ADMIN_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ECS_SECURITY_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_DIRECT_ROLE_ARN;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class ManagedPoliciesStack extends NestedStack {
    private final InstanceProperties instanceProperties;
    private final ManagedPolicy directIngestPolicy;
    private final ManagedPolicy ingestByQueuePolicy;
    private final ManagedPolicy queryPolicy;
    private final ManagedPolicy editTablesPolicy;
    private final ManagedPolicy reportingPolicy;
    private final ManagedPolicy readIngestSourcesPolicy;
    private final ManagedPolicy clearInstancePolicy;
    private final ManagedPolicy adminPolicy;
    private ManagedPolicy invokeCompactionPolicy;
    private SecurityGroup ecsSecurityGroup;

    public ManagedPoliciesStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);
        this.instanceProperties = instanceProperties;

        directIngestPolicy = createManagedPolicy("DirectIngest");
        ingestByQueuePolicy = createManagedPolicy("IngestByQueue");

        queryPolicy = createManagedPolicy("Query");
        editTablesPolicy = createManagedPolicy("EditTables");
        reportingPolicy = createManagedPolicy("Reporting");
        clearInstancePolicy = createManagedPolicy("ClearInstance");
        adminPolicy = createManagedPolicy("Admin");

        List<IBucket> sourceBuckets = addIngestSourceBucketReferences(this, instanceProperties);
        if (sourceBuckets.isEmpty()) { // CDK doesn't allow a managed policy without any grants
            readIngestSourcesPolicy = null;
        } else {
            readIngestSourcesPolicy = createManagedPolicy("ReadIngestSources");
            sourceBuckets.forEach(bucket -> bucket.grantRead(readIngestSourcesPolicy));
        }

        createEcsSecurityGroup(this, instanceProperties);
    }

    public SleeperInstanceRoles createRoles() {
        SleeperInstanceRoles roles = new SleeperInstanceRoles(
                createAdminRole(),
                createIngestByQueueRole(),
                createDirectIngestRole());
        Utils.addTags(this, instanceProperties);
        return roles;
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

    public ManagedPolicy getEditStateStoreCommitterTriggerPolicyForGrants() {
        return adminPolicy;
    }

    public ManagedPolicy getPurgeQueuesPolicyForGrants() {
        return adminPolicy;
    }

    public ManagedPolicy getInvokeCompactionPolicyForGrants() {
        // Avoid creating empty policy when we're not deploying compaction stack
        if (invokeCompactionPolicy == null) {
            invokeCompactionPolicy = createManagedPolicy("InvokeCompaction");
        }
        return invokeCompactionPolicy;
    }

    public void grantInvokeScheduled(IFunction function) {
        Utils.grantInvokeOnPolicy(function, adminPolicy);
    }

    public void grantInvokeScheduled(IFunction function, IQueue invokeQueue) {
        grantInvokeScheduled(function);
        invokeQueue.grantSendMessages(adminPolicy);
    }

    public void grantReadIngestSources(IRole role) {
        if (readIngestSourcesPolicy != null) {
            readIngestSourcesPolicy.attachToRole(role);
        }
    }

    public SecurityGroup getEcsSecurityGroup() {
        return ecsSecurityGroup;
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

    private Role createAdminRole() {
        Role role = Role.Builder.create(this, "AdminRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-admin-" + Utils.cleanInstanceId(instanceProperties))
                .build();

        instanceAdminPolicies().forEach(policy -> policy.attachToRole(role));
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        instanceProperties.set(ADMIN_ROLE_ARN, role.getRoleArn());
        return role;
    }

    private Stream<ManagedPolicy> instanceAdminPolicies() {
        return Stream.of(
                directIngestPolicy, ingestByQueuePolicy, queryPolicy,
                editTablesPolicy, reportingPolicy, clearInstancePolicy, adminPolicy,
                invokeCompactionPolicy)
                .filter(policy -> policy != null);
    }

    private Role createIngestByQueueRole() {
        Role role = Role.Builder.create(this, "IngestByQueueRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-ingest-by-queue-" + Utils.cleanInstanceId(instanceProperties))
                .build();
        ingestByQueuePolicy.attachToRole(role);
        instanceProperties.set(INGEST_BY_QUEUE_ROLE_ARN, role.getRoleArn());
        return role;
    }

    private Role createDirectIngestRole() {
        Role role = Role.Builder.create(this, "DirectIngestRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-ingest-direct-" + Utils.cleanInstanceId(instanceProperties))
                .build();
        directIngestPolicy.attachToRole(role);
        instanceProperties.set(INGEST_DIRECT_ROLE_ARN, role.getRoleArn());
        return role;
    }

    private void createEcsSecurityGroup(Construct scope, InstanceProperties instanceProperties) {
        ecsSecurityGroup = SecurityGroup.Builder.create(scope, "ECS")
                .vpc(Vpc.fromLookup(scope, "vpc", VpcLookupOptions.builder().vpcId(instanceProperties.get(VPC_ID)).build()))
                .description("Security group for ECS tasks and services that do not need to serve incomming requests.")
                .allowAllOutbound(true)
                .build();
        instanceProperties.set(ECS_SECURITY_GROUP, ecsSecurityGroup.getSecurityGroupId());
    }
}
