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
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.Role;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_ROLE;

public class ManagedPoliciesStack extends NestedStack {

    private final ManagedPolicy ingestPolicy;

    public ManagedPoliciesStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);

        ingestPolicy = new ManagedPolicy(this, "IngestPolicy");
        addIngestSourceRoleReferences(this, instanceProperties)
                .forEach(role -> role.addManagedPolicy(ingestPolicy));
    }

    public ManagedPolicy getIngestPolicy() {
        return ingestPolicy;
    }

    // WARNING: When assigning grants to these roles, the ID of the role reference is incorrectly used as the name of
    //          the IAM policy. This means the resulting ID must be unique within your AWS account. This is a bug in
    //          the CDK.
    private static List<IRole> addIngestSourceRoleReferences(Construct scope, InstanceProperties instanceProperties) {
        AtomicInteger index = new AtomicInteger(1);
        return instanceProperties.getList(INGEST_SOURCE_ROLE).stream()
                .filter(not(String::isBlank))
                .map(name -> Role.fromRoleName(scope, ingestSourceRoleReferenceId(instanceProperties, index), name))
                .collect(Collectors.toUnmodifiableList());
    }

    private static String ingestSourceRoleReferenceId(InstanceProperties instanceProperties, AtomicInteger index) {
        return Utils.truncateTo64Characters(String.join("-",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT),
                String.valueOf(index.getAndIncrement()), "IngestSourceRole"));
    }
}
