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

import software.amazon.awssdk.arns.Arn;

import sleeper.api.auth.PrincipalId;

import java.util.Optional;

/**
 * Identifies AWS compute by the IAM role it is running as.
 * <p>
 * API Gateway reports the caller as a session rather than a role, including a session name that differs on every
 * request. That is reduced here to the role itself, so a role can be granted access once rather than per session.
 */
public class IamPrincipalMapper {

    private static final String ASSUMED_ROLE = "assumed-role/";
    private static final String ROLE = "role/";
    private static final String USER = "user/";
    private static final String ROOT = "root";

    private IamPrincipalMapper() {
    }

    /**
     * Identifies the caller an ARN refers to.
     *
     * @param  callerArn the ARN reported by API Gateway
     * @return           the principal, or an empty optional if the ARN could not be read
     */
    public static Optional<PrincipalId> principalFor(String callerArn) {
        return normalise(callerArn).map(PrincipalId::forIamRole);
    }

    /**
     * Reduces an ARN to the form used to identify a principal.
     *
     * @param  arn the ARN
     * @return     the normalised ARN, or an empty optional if it could not be read
     */
    public static Optional<String> normalise(String arn) {
        if (arn == null || arn.isBlank()) {
            return Optional.empty();
        }
        try {
            return Arn.tryFromString(arn).flatMap(IamPrincipalMapper::normalise);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static Optional<String> normalise(Arn arn) {
        String resource = arn.resourceAsString();
        if (ROOT.equals(resource)) {
            return Optional.of(iamArn(arn, ROOT));
        }
        return sessionRole(resource)
                .or(() -> lastSegment(resource, ROLE))
                .map(role -> iamArn(arn, ROLE + role))
                .or(() -> lastSegment(resource, USER).map(user -> iamArn(arn, USER + user)));
    }

    /**
     * Reads the role from an assumed role, dropping the session name that follows it.
     *
     * @param  resource the resource part of an ARN
     * @return          the role, or an empty optional if this is not an assumed role
     */
    private static Optional<String> sessionRole(String resource) {
        if (!resource.startsWith(ASSUMED_ROLE)) {
            return Optional.empty();
        }
        return firstSegment(resource.substring(ASSUMED_ROLE.length()));
    }

    /**
     * Reads the name of a resource, dropping any path before it.
     *
     * @param  resource the resource part of an ARN
     * @param  prefix   the prefix the resource must start with
     * @return          the name, or an empty optional if the resource does not start with the prefix
     */
    private static Optional<String> lastSegment(String resource, String prefix) {
        if (!resource.startsWith(prefix)) {
            return Optional.empty();
        }
        String remainder = resource.substring(prefix.length());
        int lastSeparator = remainder.lastIndexOf('/');
        return nonBlank(lastSeparator < 0 ? remainder : remainder.substring(lastSeparator + 1));
    }

    private static Optional<String> firstSegment(String remainder) {
        int separator = remainder.indexOf('/');
        return nonBlank(separator < 0 ? remainder : remainder.substring(0, separator));
    }

    private static Optional<String> nonBlank(String value) {
        return value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    private static String iamArn(Arn arn, String resource) {
        return "arn:" + arn.partition() + ":iam::" + arn.accountId().orElse("") + ":" + resource;
    }

}
