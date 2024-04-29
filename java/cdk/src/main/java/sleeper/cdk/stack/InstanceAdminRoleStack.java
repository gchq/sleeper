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
import software.amazon.awscdk.services.iam.Role;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Locale;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ADMIN_ROLE_ARN;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class InstanceAdminRoleStack extends NestedStack {

    public InstanceAdminRoleStack(
            Construct scope, String id, InstanceProperties instanceProperties,
            ManagedPoliciesStack policiesStack) {
        super(scope, id);

        Role adminRole = Role.Builder.create(this, "AdminRole")
                .assumedBy(new AccountRootPrincipal())
                .roleName("sleeper-admin-" + instanceProperties.get(ID).toLowerCase(Locale.ROOT))
                .build();
        policiesStack.instanceAdminPolicies().forEach(policy -> policy.attachToRole(adminRole));
        instanceProperties.set(ADMIN_ROLE_ARN, adminRole.getRoleArn());
    }

}
