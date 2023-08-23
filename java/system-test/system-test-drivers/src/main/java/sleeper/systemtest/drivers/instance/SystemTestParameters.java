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

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

import sleeper.systemtest.cdk.SystemTestBucketStack;
import sleeper.systemtest.cdk.SystemTestClusterStack;

import java.nio.file.Path;
import java.util.Optional;

public class SystemTestParameters {

    private final String shortTestId;
    private final String account;
    private final String region;
    private final String vpcId;
    private final String subnetIds;
    private final Path scriptsDirectory;
    private final Path outputDirectory;

    private SystemTestParameters(Builder builder) {
        shortTestId = builder.shortTestId;
        account = builder.account;
        region = builder.region;
        vpcId = builder.vpcId;
        subnetIds = builder.subnetIds;
        scriptsDirectory = builder.scriptsDirectory;
        outputDirectory = builder.outputDirectory;
    }

    public static SystemTestParameters loadFromSystemProperties() {
        return builder()
                .account(AWSSecurityTokenServiceClientBuilder.defaultClient()
                        .getCallerIdentity(new GetCallerIdentityRequest()).getAccount())
                .region(new DefaultAwsRegionProviderChain().getRegion().id())
                .shortTestId(System.getProperty("sleeper.system.test.short.id"))
                .vpcId(System.getProperty("sleeper.system.test.vpc.id"))
                .subnetIds(System.getProperty("sleeper.system.test.subnet.ids"))
                .scriptsDirectory(findScriptsDir())
                .outputDirectory(Optional.ofNullable(System.getProperty("sleeper.system.test.output.dir"))
                        .map(Path::of)
                        .orElse(null))
                .build();
    }

    public String getSystemTestDeploymentId() {
        return shortTestId;
    }

    public String buildInstanceId(String identifier) {
        return shortTestId + "-" + identifier;
    }

    public String buildSystemTestBucketName() {
        return SystemTestBucketStack.buildSystemTestBucketName(shortTestId);
    }

    public String buildSystemTestWriterRoleName() {
        return SystemTestClusterStack.buildSystemTestWriterRoleName(shortTestId);
    }

    public String buildJarsBucketName() {
        return String.format("sleeper-%s-jars", shortTestId);
    }

    public String getAccount() {
        return account;
    }

    public String getRegion() {
        return region;
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

    public Path getJarsDirectory() {
        return scriptsDirectory.resolve("jars");
    }

    public Path getDockerDirectory() {
        return scriptsDirectory.resolve("docker");
    }

    public Path getGeneratedDirectory() {
        return scriptsDirectory.resolve("generated");
    }

    public Path getOutputDirectory() {
        return outputDirectory;
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
        private String account;
        private String region;
        private String vpcId;
        private String subnetIds;
        private Path scriptsDirectory;
        private Path outputDirectory;

        private Builder() {
        }

        public Builder shortTestId(String shortTestId) {
            this.shortTestId = shortTestId;
            return this;
        }

        public Builder account(String account) {
            this.account = account;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
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

        public Builder outputDirectory(Path outputDirectory) {
            this.outputDirectory = outputDirectory;
            return this;
        }

        public SystemTestParameters build() {
            return new SystemTestParameters(this);
        }
    }
}
