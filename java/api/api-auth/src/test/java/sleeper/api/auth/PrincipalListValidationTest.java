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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class PrincipalListValidationTest {

    @ParameterizedTest(name = "{0} valid={1}")
    @MethodSource("principals")
    void shouldValidatePrincipalTheSameWayTheApiReadsIt(String principal, boolean expectedValid) {
        assertThat(SleeperPropertyValueUtils.isValidPrincipalList(principal))
                .describedAs("valid according to instance property validation")
                .isEqualTo(expectedValid);
        assertThat(canParse(principal))
                .describedAs("readable by the API")
                .isEqualTo(expectedValid);
    }

    private static Stream<Arguments> principals() {
        return Stream.of(
                // Roles
                arguments("iam#arn:aws:iam::123456789012:role/SleeperAdmin", true),
                arguments("iam#arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/eu-west-2/AWSReservedSSO_Admin_abc", true),
                arguments("iam#arn:aws:sts::123456789012:assumed-role/SleeperAdmin/some-session", true),
                arguments("iam#arn:aws:sts::123456789012:assumed-role/SleeperAdmin", true),
                arguments("iam#arn:aws-us-gov:iam::123456789012:role/SleeperAdmin", true),
                arguments("iam#arn:aws:iam::123456789012:role/", false),
                arguments("iam#arn:aws:iam::123456789012:role/path/", false),
                arguments("iam#arn:aws:sts::123456789012:assumed-role/", false),
                arguments("iam#arn:aws:sts::123456789012:assumed-role//some-session", false),
                // Users and the account root
                arguments("iam#arn:aws:iam::123456789012:user/someone", true),
                arguments("iam#arn:aws:iam::123456789012:user/path/someone", true),
                arguments("iam#arn:aws:iam::123456789012:user/", false),
                arguments("iam#arn:aws:iam::123456789012:root", true),
                // Not an ARN a caller could be identified by
                arguments("iam#SleeperAdmin", false),
                arguments("iam#arn:aws:s3:::some-bucket", false),
                arguments("iam#arn:aws:iam::123456789012", false),
                arguments("iam#arn::iam::123456789012:role/SleeperAdmin", false),
                arguments("iam#arn:aws:::123456789012:role/SleeperAdmin", false),
                arguments("iam#arn:aws:iam::123456789012:", false),
                arguments("iam#ARN:aws:iam::123456789012:role/SleeperAdmin", false),
                // Token subjects
                arguments("jwt#https://idp.internal/realms/sleeper#a1b2c3", true),
                arguments("jwt#https://idp.internal/realms/sleeper#a1b2#c3", true),
                arguments("jwt#https://idp.internal/realms/sleeper", false),
                arguments("jwt##a1b2c3", false),
                arguments("jwt# #a1b2c3", false),
                arguments("jwt#https://idp.internal/realms/sleeper#", false),
                arguments("jwt#https://idp.internal/realms/sleeper# ", false),
                // No type, or an unknown type
                arguments("arn:aws:iam::123456789012:role/SleeperAdmin", false),
                arguments("saml#something", false),
                arguments("iam#", false),
                arguments("jwt#", false),
                arguments("#something", false));
    }

    private static boolean canParse(String principal) {
        try {
            PrincipalId.parse(principal);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
