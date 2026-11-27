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
package sleeper.api.auth;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.api.auth.SleeperPermission.ADMIN;
import static sleeper.api.auth.SleeperPermission.READ_INSTANCE_CONFIGURATION;
import static sleeper.api.auth.SleeperPermission.READ_TABLE_CONFIGURATION;

public class PrincipalPermissionsTest {

    @Nested
    @DisplayName("Allow nothing by default")
    class AllowNothingByDefault {

        @Test
        void shouldAllowNothingForAPrincipalThatHasNotBeenGrantedAnything() {
            PrincipalPermissions permissions = PrincipalPermissions.none();

            assertThat(permissions.allows(READ_INSTANCE_CONFIGURATION)).isFalse();
            assertThat(permissions.allows(READ_TABLE_CONFIGURATION, "table1")).isFalse();
            assertThat(permissions.isAdmin()).isFalse();
        }

        @Test
        void shouldAllowNothingWhenGrantedNothing() {
            assertThat(PrincipalPermissions.of(List.of())).isEqualTo(PrincipalPermissions.none());
        }
    }

    @Nested
    @DisplayName("Allow a permission that applies to the instance")
    class AllowInstancePermission {

        @Test
        void shouldAllowAGrantedPermission() {
            assertThat(permissions(PermissionGrant.of(READ_INSTANCE_CONFIGURATION))
                    .allows(READ_INSTANCE_CONFIGURATION)).isTrue();
        }

        @Test
        void shouldRefuseAPermissionThatWasNotGranted() {
            assertThat(PrincipalPermissions.none().allows(READ_INSTANCE_CONFIGURATION)).isFalse();
        }
    }

    @Nested
    @DisplayName("Allow a permission against particular tables")
    class AllowTablePermission {

        @Test
        void shouldAllowATableItWasGrantedOver() {
            assertThat(readTableConfiguration(TableScope.tables(List.of("events", "customers")))
                    .allows(READ_TABLE_CONFIGURATION, "events")).isTrue();
        }

        @Test
        void shouldRefuseATableItWasNotGrantedOver() {
            assertThat(readTableConfiguration(TableScope.tables(List.of("events", "customers")))
                    .allows(READ_TABLE_CONFIGURATION, "metrics")).isFalse();
        }

        @Test
        void shouldAllowAnyTableWhenGrantedOverAllTables() {
            PrincipalPermissions permissions = readTableConfiguration(TableScope.allTables());

            assertThat(permissions.allows(READ_TABLE_CONFIGURATION, "events")).isTrue();
            assertThat(permissions.allows(READ_TABLE_CONFIGURATION, "a-table-added-later")).isTrue();
        }

        @Test
        void shouldNotAllowAPermissionAcrossTheInstanceFromAGrantOverParticularTables() {
            PermissionGrant grant = PermissionGrant.of(READ_TABLE_CONFIGURATION, TableScope.tables(List.of("events")));

            assertThat(grant.allows(READ_TABLE_CONFIGURATION)).isFalse();
        }
    }

    @Nested
    @DisplayName("Allow everything to an administrator")
    class AllowEverythingToAdministrator {

        @Test
        void shouldAllowAPermissionThatWasNeverGrantedExplicitly() {
            assertThat(PrincipalPermissions.admin().allows(READ_INSTANCE_CONFIGURATION)).isTrue();
        }

        @Test
        void shouldAllowEveryTable() {
            assertThat(PrincipalPermissions.admin().allows(READ_TABLE_CONFIGURATION, "any-table")).isTrue();
        }

        @Test
        void shouldReportBeingAnAdministrator() {
            assertThat(PrincipalPermissions.admin().isAdmin()).isTrue();
            assertThat(permissions(PermissionGrant.of(READ_INSTANCE_CONFIGURATION)).isAdmin()).isFalse();
        }
    }

    @Nested
    @DisplayName("Refuse a check that could not be answered correctly")
    class RefuseCheckThatCannotBeAnswered {

        @Test
        void shouldRefuseToCheckATablePermissionWithoutATable() {
            assertThatThrownBy(() -> PrincipalPermissions.admin().allows(READ_TABLE_CONFIGURATION))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldRefuseToCheckAnInstancePermissionAgainstATable() {
            assertThatThrownBy(() -> PrincipalPermissions.admin().allows(READ_INSTANCE_CONFIGURATION, "table1"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldRefuseToGrantATablePermissionWithoutTables() {
            assertThatThrownBy(() -> PermissionGrant.of(READ_TABLE_CONFIGURATION))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldRefuseToGrantAnInstancePermissionOverTables() {
            assertThatThrownBy(() -> PermissionGrant.of(READ_INSTANCE_CONFIGURATION, TableScope.allTables()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldRefuseToGrantOverNoTables() {
            assertThatThrownBy(() -> TableScope.tables(List.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Keep administrator separate from other permissions")
    class KeepAdministratorSeparate {

        @Test
        void shouldNotTreatAdministratorAsATablePermission() {
            assertThat(ADMIN.isTableScoped()).isFalse();
        }

        @Test
        void shouldNotAllowAdministratorThroughAnotherPermission() {
            assertThat(permissions(PermissionGrant.of(READ_INSTANCE_CONFIGURATION)).allows(ADMIN)).isFalse();
        }
    }

    private static PrincipalPermissions permissions(PermissionGrant... grants) {
        return PrincipalPermissions.of(List.of(grants));
    }

    private static PrincipalPermissions readTableConfiguration(TableScope tables) {
        return permissions(PermissionGrant.of(READ_TABLE_CONFIGURATION, tables));
    }

}
