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
package sleeper.api.auth.iam;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import sleeper.api.auth.PrincipalId;

import static org.assertj.core.api.Assertions.assertThat;

public class IamPrincipalMapperTest {

    @Nested
    @DisplayName("Identify the role behind a session")
    class IdentifyRoleBehindSession {

        @Test
        void shouldReduceAnAssumedRoleToTheRole() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Ingest/i-0abc123"))
                    .contains("arn:aws:iam::123456789012:role/Ingest");
        }

        @Test
        void shouldGiveTheSameResultForASessionAndTheRoleItAssumed() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Ingest/i-0abc123"))
                    .isEqualTo(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:role/Ingest"));
        }

        @Test
        void shouldGiveADifferentResultForADifferentSessionOfTheSameRole() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Ingest/session-one"))
                    .isEqualTo(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Ingest/session-two"));
        }

        @Test
        void shouldNotConfuseTwoRolesWithDifferentNames() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Ingest/session"))
                    .isNotEqualTo(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Compaction/session"));
        }

        @Test
        void shouldNotConfuseTheSameRoleNameInDifferentAccounts() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:sts::111111111111:assumed-role/Ingest/session"))
                    .isNotEqualTo(IamPrincipalMapper.normalise("arn:aws:sts::222222222222:assumed-role/Ingest/session"));
        }
    }

    @Nested
    @DisplayName("Work in any partition")
    class WorkInAnyPartition {

        @ParameterizedTest
        @ValueSource(strings = {"aws", "aws-us-gov", "aws-iso"})
        void shouldReduceAnAssumedRoleInEveryPartition(String partition) {
            assertThat(IamPrincipalMapper.normalise(
                    "arn:" + partition + ":sts::123456789012:assumed-role/Ingest/session"))
                    .contains("arn:" + partition + ":iam::123456789012:role/Ingest");
        }

        @Test
        void shouldNotConfuseTheSameRoleInDifferentPartitions() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Ingest/session"))
                    .isNotEqualTo(IamPrincipalMapper.normalise("arn:aws-iso:sts::123456789012:assumed-role/Ingest/session"));
        }
    }

    @Nested
    @DisplayName("Ignore a role's path")
    class IgnoreRolePath {

        @Test
        void shouldDropThePathFromARole() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:role/sleeper/ingest/Ingest"))
                    .contains("arn:aws:iam::123456789012:role/Ingest");
        }

        @Test
        void shouldMatchARoleWithAPathAgainstASessionOfThatRole() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:role/sleeper/ingest/Ingest"))
                    .isEqualTo(IamPrincipalMapper.normalise("arn:aws:sts::123456789012:assumed-role/Ingest/session"));
        }

        @Test
        void shouldLeaveARoleWithNoPathAlone() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:role/Ingest"))
                    .contains("arn:aws:iam::123456789012:role/Ingest");
        }
    }

    @Nested
    @DisplayName("Identify other kinds of caller")
    class IdentifyOtherCallers {

        @Test
        void shouldIdentifyAnIamUser() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:user/some-user"))
                    .contains("arn:aws:iam::123456789012:user/some-user");
        }

        @Test
        void shouldDropThePathFromAnIamUser() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:user/engineering/some-user"))
                    .contains("arn:aws:iam::123456789012:user/some-user");
        }

        @Test
        void shouldIdentifyTheAccountRoot() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:root"))
                    .contains("arn:aws:iam::123456789012:root");
        }

        @Test
        void shouldNotConfuseAUserWithARoleOfTheSameName() {
            assertThat(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:user/Ingest"))
                    .isNotEqualTo(IamPrincipalMapper.normalise("arn:aws:iam::123456789012:role/Ingest"));
        }
    }

    @Nested
    @DisplayName("Refuse anything that cannot be read")
    class RefuseUnreadable {

        @ParameterizedTest
        @ValueSource(strings = {
            "not an arn",
            "arn:aws:iam::123456789012",
            "arn:aws:sts::123456789012:assumed-role/",
            "arn:aws:iam::123456789012:role/",
            "arn:aws:iam::123456789012:group/some-group",
            "arn:aws:s3:::some-bucket",
            ""})
        void shouldRefuseAnArnItCannotIdentifyACallerFrom(String arn) {
            assertThat(IamPrincipalMapper.normalise(arn)).isEmpty();
        }

        @Test
        void shouldRefuseNothing() {
            assertThat(IamPrincipalMapper.normalise(null)).isEmpty();
        }
    }

    @Nested
    @DisplayName("Build a principal")
    class BuildPrincipal {

        @Test
        void shouldIdentifyTheCallerOfARequest() {
            assertThat(IamPrincipalMapper.principalFor("arn:aws:sts::123456789012:assumed-role/Ingest/session"))
                    .contains(PrincipalId.forIamRole("arn:aws:iam::123456789012:role/Ingest"));
        }

        @Test
        void shouldNotIdentifyACallerFromAnUnreadableArn() {
            assertThat(IamPrincipalMapper.principalFor("not an arn")).isEmpty();
        }
    }
}
