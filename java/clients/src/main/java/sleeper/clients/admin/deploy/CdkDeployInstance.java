/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.admin.deploy;

import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public class CdkDeployInstance {

    private final Path instancePropertiesFile;
    private final Path jarsDirectory;
    private final Path cdkJarFile;
    private final String cdkAppClassName;
    private final boolean ensureNewInstance;

    private CdkDeployInstance(Builder builder) {
        instancePropertiesFile = requireNonNull(builder.instancePropertiesFile, "instancePropertiesFile must not be null");
        jarsDirectory = requireNonNull(builder.jarsDirectory, "jarsDirectory must not be null");
        cdkJarFile = requireNonNull(builder.cdkJarFile, "cdkJarFile must not be null");
        cdkAppClassName = requireNonNull(builder.cdkAppClassName, "cdkAppClassName must not be null");
        ensureNewInstance = builder.ensureNewInstance;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void deploy() throws IOException, InterruptedException {
        int exitCode = ClientUtils.runCommand("cdk",
                "-a", String.format("java -cp \"%s\" %s", cdkJarFile, cdkAppClassName),
                "deploy",
                "--require-approval", "never",
                "-c", String.format("propertiesfile=%s", instancePropertiesFile),
                "-c", String.format("jarsdir=%s", jarsDirectory),
                "-c", String.format("newinstance=%s", ensureNewInstance),
                "*");

        if (exitCode != 0) {
            throw new IOException("Failed in cdk deploy");
        }
    }

    public static final class Builder {
        private Path instancePropertiesFile;
        private Path jarsDirectory;
        private Path cdkJarFile;
        private String cdkAppClassName;
        private boolean ensureNewInstance;

        private Builder() {
        }

        public Builder instancePropertiesFile(Path instancePropertiesFile) {
            this.instancePropertiesFile = instancePropertiesFile;
            return this;
        }

        public Builder jarsDirectory(Path jarsDirectory) {
            this.jarsDirectory = jarsDirectory;
            return this;
        }

        public Builder cdkJarFile(Path cdkJarFile) {
            this.cdkJarFile = cdkJarFile;
            return this;
        }

        public Builder cdkAppClassName(String cdkAppClassName) {
            this.cdkAppClassName = cdkAppClassName;
            return this;
        }

        public Builder ensureNewInstance(boolean ensureNewInstance) {
            this.ensureNewInstance = ensureNewInstance;
            return this;
        }

        public CdkDeployInstance build() {
            return new CdkDeployInstance(this);
        }
    }
}
