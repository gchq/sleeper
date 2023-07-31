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

package sleeper.systemtest.drivers.instance;

import java.nio.file.Path;

public class SystemTestParameters {

    private final String shortTestId;
    private final String vpcId;
    private final String subnetIds;
    private final Path scriptsDirectory;

    private SystemTestParameters(Builder builder) {
        shortTestId = builder.shortTestId;
        vpcId = builder.vpcId;
        subnetIds = builder.subnetIds;
        scriptsDirectory = builder.scriptsDirectory;
    }

    public static SystemTestParameters loadFromSystemProperties() {
        return builder()
                .shortTestId(System.getProperty("sleeper.system.test.short.id"))
                .vpcId(System.getProperty("sleeper.system.test.vpc.id"))
                .subnetIds(System.getProperty("sleeper.system.test.subnet.ids"))
                .scriptsDirectory(findScriptsDir())
                .build();
    }

    public String buildInstanceId(String identifier) {
        return shortTestId + "-" + identifier;
    }

    public String buildSourceBucketName() {
        return "sleeper-" + shortTestId + "-ingest-source-bucket";
    }

    public String getVpcId() {
        return vpcId;
    }

    public String getSubnetIds() {
        return subnetIds;
    }

    public Path getScriptsDirectory() {
        return scriptsDirectory;
    }

    private static Builder builder() {
        return new Builder();
    }

    private static Path findScriptsDir() {
        return getParentOrFail(findJavaDir()).resolve("scripts");
    }

    private static Path findJavaDir() {
        return findJavaDir(Path.of(".").toAbsolutePath());
    }

    private static Path findJavaDir(Path currentPath) {
        if ("java".equals(String.valueOf(currentPath.getFileName()))) {
            return currentPath;
        } else {
            return findJavaDir(getParentOrFail(currentPath));
        }
    }

    private static Path getParentOrFail(Path path) {
        Path parent = path.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("No parent of path " + path);
        } else {
            return parent;
        }
    }

    public static final class Builder {
        private String shortTestId;
        private String vpcId;
        private String subnetIds;
        private Path scriptsDirectory;

        private Builder() {
        }

        public Builder shortTestId(String shortTestId) {
            this.shortTestId = shortTestId;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetIds(String subnetIds) {
            this.subnetIds = subnetIds;
            return this;
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public SystemTestParameters build() {
            return new SystemTestParameters(this);
        }
    }
}
