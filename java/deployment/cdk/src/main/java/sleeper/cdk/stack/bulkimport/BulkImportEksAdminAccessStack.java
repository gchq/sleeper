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
package sleeper.cdk.stack.bulkimport;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.eks_v2.AccessEntry;
import software.amazon.awscdk.services.eks_v2.AccessPolicy;
import software.amazon.awscdk.services.eks_v2.AccessPolicyNameOptions;
import software.amazon.awscdk.services.eks_v2.AccessScopeType;
import software.amazon.awscdk.services.eks_v2.Cluster;
import software.amazon.awscdk.services.iam.IRole;
import software.constructs.Construct;

import java.util.List;

/**
 * Grants the instance admin role cluster admin access on the EKS bulk import cluster.
 * <p>
 * Lives in its own nested stack — downstream of both EksBulkImportStack (cluster) and the managed policies
 * stack (admin role) — so that the access entry's references to those stacks don't form a circular dependency.
 */
public class BulkImportEksAdminAccessStack extends NestedStack {

    public BulkImportEksAdminAccessStack(Construct scope, String id, Cluster cluster, IRole role) {
        super(scope, id);

        AccessEntry.Builder.create(this, "ClusterAdminAccess")
                .cluster(cluster)
                .principal(role.getRoleArn())
                .accessPolicies(List.of(
                        AccessPolicy.fromAccessPolicyName("AmazonEKSClusterAdminPolicy",
                                AccessPolicyNameOptions.builder()
                                        .accessScopeType(AccessScopeType.CLUSTER)
                                        .build())))
                .build();
    }

}
