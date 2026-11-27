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

import java.util.Arrays;
import java.util.Optional;

/**
 * How a client proved who they are. This is part of a principal's identifier, so that a role ARN and a token subject
 * can never be confused with one another when looking up what a client is allowed to do.
 */
public enum PrincipalType {

    /** A client that presented a token from a configured OIDC issuer. */
    JWT("jwt"),

    /** AWS compute that signed its request with the IAM role it is running as. */
    IAM("iam");

    private final String prefix;

    PrincipalType(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    /**
     * Finds the type with the given prefix.
     *
     * @param  prefix the prefix
     * @return        the type, or an empty optional if no type has that prefix
     */
    public static Optional<PrincipalType> fromPrefix(String prefix) {
        return Arrays.stream(values())
                .filter(type -> type.prefix.equals(prefix))
                .findFirst();
    }
}
