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
package sleeper.api.auth.store;

import org.junit.jupiter.api.Test;

import sleeper.api.auth.PrincipalId;
import sleeper.api.auth.iam.IamPrincipalMapper;
import sleeper.core.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.ApiProperty.API_AUTH_ADMIN_PRINCIPALS;

public class AdminPrincipalsStoreTest {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/SleeperAdmin";
    private static final String ISSUER = "https://idp.internal/realms/sleeper";

    private final InstanceProperties properties = new InstanceProperties();

    @Test
    void shouldAllowNobodyWhenNoAdministratorsAreListed() {
        assertThat(store().permissionsFor(PrincipalId.forIamRole(ROLE_ARN)).isAdmin()).isFalse();
    }

    @Test
    void shouldMakeAListedRoleAnAdministrator() {
        properties.set(API_AUTH_ADMIN_PRINCIPALS, "iam#" + ROLE_ARN);

        assertThat(store().permissionsFor(PrincipalId.forIamRole(ROLE_ARN)).isAdmin()).isTrue();
    }

    @Test
    void shouldMakeAListedRoleWithAPathAnAdministratorWhenCallingAsASessionOfThatRole() {
        properties.set(API_AUTH_ADMIN_PRINCIPALS,
                "iam#arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/eu-west-2/AWSReservedSSO_Admin_abc");

        PrincipalId caller = IamPrincipalMapper.principalFor(
                "arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_Admin_abc/[EMAIL_REDACTED]").orElseThrow();
        assertThat(store().permissionsFor(caller).isAdmin()).isTrue();
    }

    @Test
    void shouldMakeAListedTokenSubjectAnAdministrator() {
        properties.set(API_AUTH_ADMIN_PRINCIPALS, "jwt#" + ISSUER + "#a1b2c3");

        assertThat(store().permissionsFor(PrincipalId.forToken(ISSUER, "a1b2c3")).isAdmin()).isTrue();
    }

    @Test
    void shouldAllowNothingToAPrincipalThatIsNotListed() {
        properties.set(API_AUTH_ADMIN_PRINCIPALS, "iam#" + ROLE_ARN);

        assertThat(store().permissionsFor(PrincipalId.forToken(ISSUER, "a1b2c3")).isEmpty()).isTrue();
    }

    @Test
    void shouldNotMatchTheSameSubjectFromADifferentIssuer() {
        properties.set(API_AUTH_ADMIN_PRINCIPALS, "jwt#" + ISSUER + "#a1b2c3");

        assertThat(store().permissionsFor(PrincipalId.forToken("https://somewhere.else", "a1b2c3")).isAdmin()).isFalse();
    }

    @Test
    void shouldListSeveralAdministrators() {
        properties.set(API_AUTH_ADMIN_PRINCIPALS, "iam#" + ROLE_ARN + ",jwt#" + ISSUER + "#a1b2c3");

        assertThat(store().permissionsFor(PrincipalId.forIamRole(ROLE_ARN)).isAdmin()).isTrue();
        assertThat(store().permissionsFor(PrincipalId.forToken(ISSUER, "a1b2c3")).isAdmin()).isTrue();
    }

    @Test
    void shouldIgnoreAPrincipalThatCannotBeReadAndKeepTheOthers() {
        properties.set(API_AUTH_ADMIN_PRINCIPALS, "arn:aws:iam::123456789012:role/Typo,iam#" + ROLE_ARN);

        assertThat(store().permissionsFor(PrincipalId.forIamRole(ROLE_ARN)).isAdmin()).isTrue();
        assertThat(store().permissionsFor(PrincipalId.forIamRole("arn:aws:iam::123456789012:role/Typo")).isAdmin()).isFalse();
    }

    private AdminPrincipalsStore store() {
        return AdminPrincipalsStore.from(properties);
    }

}
