/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.deploy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Definitions of jar files used to deploy client functions.
 */
public class ClientJar {

    private static final List<ClientJar> ALL = new ArrayList<>();

    public static final ClientJar BULK_IMPORT_RUNNER = builder().filenameFormat("bulk-import-runner-%s.jar").artifactId("bulk-import-runner").add();
    public static final ClientJar CDK = builder().filenameFormat("cdk-%s.jar").artifactId("cdk").add();
    public static final ClientJar CLIENTS_UTILITY = builder().filenameFormat("clients-%s-utility.jar").artifactId("clients").add();

    private final String filenameFormat;
    private final String artifactId;

    public ClientJar(Builder builder) {
        this.filenameFormat = builder.filenameFormat;
        this.artifactId = builder.artifactId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getFilenameFormat() {
        return filenameFormat;
    }

    public String getArtifactId() {
        return artifactId;
    }

    @Override
    public String toString() {
        return "ClientJar{filename=" + filenameFormat + ", imageName=" + artifactId + "}";
    }

    public static List<ClientJar> getAll() {
        return Collections.unmodifiableList(ALL);
    }

    /**
     * Builder for the client jar class.
     */
    public static class Builder {
        private String filenameFormat;
        private String artifactId;

        private Builder() {

        }

        /**
         * The filename format with space for version.
         *
         * @param  filenameFormat String for filename with space for version to be added
         * @return                Builder
         */
        public Builder filenameFormat(String filenameFormat) {
            this.filenameFormat = filenameFormat;
            return this;
        }

        /**
         * The artifact ID for the jar.
         *
         * @param  artifactId String artifactId
         * @return            Builder.
         */
        public Builder artifactId(String artifactId) {
            this.artifactId = artifactId;
            return this;
        }

        public ClientJar build() {
            return new ClientJar(this);
        }

        /**
         * Builds the client jar object and adds it to the ALL List.
         *
         * @return ClientJar
         */
        public ClientJar add() {
            ClientJar clientJar = build();
            ALL.add(clientJar);
            return clientJar;
        }
    }
}
