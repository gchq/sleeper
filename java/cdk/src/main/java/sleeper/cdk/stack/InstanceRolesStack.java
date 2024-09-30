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
import software.amazon.awscdk.services.iam.AccountRootPrincipal;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.Role;
import software.constructs.Construct;

import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ADMIN_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_DIRECT_ROLE_ARN;

public class InstanceRolesStack extends NestedStack {
    public InstanceRolesStack(
            Construct scope, String id, InstanceProperties instanceProperties,
            ManagedPoliciesStack policiesStack) {
        super(scope, id);

        createAdminRole(instanceProperties, policiesStack);
        createIngestByQueueRole(instanceProperties, policiesStack);
        createDirectIngestRole(instanceProperties, policiesStack);
    }

    private void createAdminRole(InstanceProperties instanceProperties, ManagedPoliciesStack policiesStack) {
        Role adminRole = Role.Builder.create(this, "AdminRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-admin-" + Utils.cleanInstanceId(instanceProperties))
                .build();

        policiesStack.instanceAdminPolicies().forEach(policy -> policy.attachToRole(adminRole));
        adminRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        instanceProperties.set(ADMIN_ROLE_ARN, adminRole.getRoleArn());
    }

    private void createIngestByQueueRole(InstanceProperties instanceProperties, ManagedPoliciesStack policiesStack) {
        Role ingestRole = Role.Builder.create(this, "IngestByQueueRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-ingest-by-queue-" + Utils.cleanInstanceId(instanceProperties))
                .build();
        policiesStack.getIngestByQueuePolicyForGrants().attachToRole(ingestRole);
        instanceProperties.set(INGEST_BY_QUEUE_ROLE_ARN, ingestRole.getRoleArn());
    }

    private void createDirectIngestRole(InstanceProperties instanceProperties, ManagedPoliciesStack policiesStack) {
        Role ingestRole = Role.Builder.create(this, "DirectIngestRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-ingest-direct-" + Utils.cleanInstanceId(instanceProperties))
                .build();
        policiesStack.getDirectIngestPolicyForGrants().attachToRole(ingestRole);
        instanceProperties.set(INGEST_DIRECT_ROLE_ARN, ingestRole.getRoleArn());
    }
}
