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

import sleeper.api.auth.iam.IamPrincipalMapper;

import java.util.Objects;

/**
 * Identifies a client of the Sleeper REST API. This is the key used to look up what a client is allowed to do.
 * <p>
 * A client that authenticated with a token is identified by the issuer as well as the subject, written as
 * {@code jwt#<issuer>#<subject>}. AWS compute is identified by its role, written as {@code iam#<role ARN>}.
 */
public class PrincipalId {

    private static final String SEPARATOR = "#";

    private final PrincipalType type;
    private final String id;

    private PrincipalId(PrincipalType type, String id) {
        this.type = type;
        this.id = id;
    }

    /**
     * Creates an identifier for AWS compute calling the API as an IAM role.
     *
     * @param  roleArn the ARN of the role, normalised by {@link sleeper.api.auth.iam.IamPrincipalMapper}
     * @return         the identifier
     */
    public static PrincipalId forIamRole(String roleArn) {
        requireSet(roleArn, "role ARN");
        return new PrincipalId(PrincipalType.IAM, PrincipalType.IAM.getPrefix() + SEPARATOR + roleArn);
    }

    /**
     * Creates an identifier for a client that presented a token.
     *
     * @param  issuer  the issuer that assigned the subject, from the token's iss claim
     * @param  subject the identifier of the client within that issuer
     * @return         the identifier
     */
    public static PrincipalId forToken(String issuer, String subject) {
        requireSet(issuer, "issuer");
        requireSet(subject, "subject");
        return new PrincipalId(PrincipalType.JWT, PrincipalType.JWT.getPrefix() + SEPARATOR + issuer + SEPARATOR + subject);
    }

    /**
     * Reads an identifier written in configuration.
     *
     * @param  id                       the identifier
     * @return                          the identifier
     * @throws IllegalArgumentException if it does not say how the client authenticates, or if it is an ARN that
     *                                  a caller could not be identified by
     */
    public static PrincipalId parse(String id) {
        requireSet(id, "principal");
        int separator = id.indexOf(SEPARATOR);
        if (separator < 1 || separator == id.length() - 1) {
            throw new IllegalArgumentException(unreadable(id));
        }
        PrincipalType type = PrincipalType.fromPrefix(id.substring(0, separator))
                .orElseThrow(() -> new IllegalArgumentException(unreadable(id)));
        String value = id.substring(separator + 1);
        if (type == PrincipalType.IAM) {
            return IamPrincipalMapper.principalFor(value)
                    .orElseThrow(() -> new IllegalArgumentException("Could not read the ARN in the principal \"" + id +
                            "\". Expected the ARN of an IAM role or user, such as " +
                            "iam#arn:aws:iam::123456789012:role/name."));
        }
        int subjectSeparator = value.indexOf(SEPARATOR);
        if (subjectSeparator < 1 || value.substring(0, subjectSeparator).isBlank()
                || value.substring(subjectSeparator + 1).isBlank()) {
            throw new IllegalArgumentException("Could not read the issuer and subject in the principal \"" + id +
                    "\". Expected jwt#<issuer>#<subject>.");
        }
        return forToken(value.substring(0, subjectSeparator), value.substring(subjectSeparator + 1));
    }

    public PrincipalType getType() {
        return type;
    }

    private static String unreadable(String id) {
        return "Could not read the principal \"" + id + "\". " +
                "Valid formats are iam#<role ARN>, or jwt#<issuer>#<subject>.";
    }

    private static void requireSet(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("A principal must set the " + field + ".");
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PrincipalId)) {
            return false;
        }
        return id.equals(((PrincipalId) object).id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return id;
    }
}
