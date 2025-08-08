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
 * Definitions of client jar files.
 */
public class ClientJar {

    private static final List<ClientJar> ALL = new ArrayList<>();

    public static final ClientJar BULK_IMPORT_RUNNER = builder()
            .filenameFormat("bulk-import-runner-%s.jar")
            .artifactID("bulk-import-runner")
            .add();
    public static final ClientJar CDK = builder()
            .filenameFormat("cdk-%s.jar")
            .artifactID("cdk")
            .add();
    public static final ClientJar CLIENTS_UTILITY = builder()
            .filenameFormat("clients-%s-utility.jar")
            .artifactID("clients")
            .add();

    private final String filenameFormat;
    private final String artifactID;

    public ClientJar(Builder builder) {
        this.filenameFormat = builder.filenameFormat;
        this.artifactID = builder.artifactID;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getFilenameFormat() {
        return filenameFormat;
    }

    public String getArtifactID() {
        return artifactID;
    }

    /**
     * Formats the filename with the supplied version.
     *
     * @param  version the version of Sleeper to use in the format
     * @return         a formatted filename with the version in it
     */
    public String getFormattedFilename(String version) {
        return String.format(filenameFormat, version);
    }

    @Override
    public String toString() {
        return "ClientJar{filename=" + filenameFormat + ", imageName=" + artifactID + "}";
    }

    public static List<ClientJar> getAll() {
        return Collections.unmodifiableList(ALL);
    }

    /**
     * Builder for the client jar class.
     */
    public static class Builder {
        private String filenameFormat;
        private String artifactID;

        private Builder() {

        }

        /**
         * Sets the filename format with space for version.
         * String.format is used to add the version to the filename.
         *
         * @param  filenameFormat the filename format with space for version to be added
         * @return                the builder for method chaining
         */
        public Builder filenameFormat(String filenameFormat) {
            this.filenameFormat = filenameFormat;
            return this;
        }

        /**
         * Sets the artifact ID for the jar.
         *
         * @param  artifactID the ID of the artefact
         * @return            the builder for method chaining
         */
        public Builder artifactID(String artifactID) {
            this.artifactID = artifactID;
            return this;
        }

        public ClientJar build() {
            return new ClientJar(this);
        }

        /**
         * Builds the client jar object and adds it to the ALL List.
         *
         * @return client jar built using current variables
         */
        public ClientJar add() {
            ClientJar clientJar = build();
            ALL.add(clientJar);
            return clientJar;
        }
    }
}
