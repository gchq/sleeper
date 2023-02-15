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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.requireNonEmpty;
import static sleeper.configuration.properties.InstanceProperties.getConfigBucketFromInstanceId;
import static sleeper.configuration.properties.SleeperProperties.loadProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

public class GenerateInstanceProperties {
    private final AmazonS3 s3;
    private final AWSSecurityTokenService sts;
    private final String instanceId;
    private final String sleeperVersion;
    private final String vpcId;
    private final String subnetId;
    private final Properties properties;
    private final Properties tagsProperties;

    private GenerateInstanceProperties(Builder builder) {
        s3 = requireNonNull(builder.s3, "s3 must not be null");
        sts = requireNonNull(builder.sts, "sts must not be null");
        instanceId = requireNonEmpty(builder.instanceId, "instanceId must not be empty");
        sleeperVersion = requireNonEmpty(builder.sleeperVersion, "sleeperVersion must not be empty");
        vpcId = requireNonEmpty(builder.vpcId, "vpcId must not be empty");
        subnetId = requireNonEmpty(builder.subnetId, "subnetId must not be empty");
        properties = Optional.ofNullable(builder.properties).orElseGet(Properties::new);
        tagsProperties = Optional.ofNullable(builder.tagsProperties).orElseGet(Properties::new);
    }

    public static Builder builder() {
        return new Builder();
    }

    public InstanceProperties generate() {
        InstanceProperties instanceProperties = new InstanceProperties(properties);
        instanceProperties.loadTags(tagsProperties);
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, getConfigBucketFromInstanceId(instanceId));
        instanceProperties.set(JARS_BUCKET, String.format("sleeper-%s-jars", instanceId));
        instanceProperties.set(ACCOUNT, getAccount());
        instanceProperties.set(REGION, s3.getRegionName());
        instanceProperties.set(VERSION, sleeperVersion);
        instanceProperties.set(VPC_ID, vpcId);
        instanceProperties.set(SUBNET, subnetId);
        instanceProperties.set(ECR_COMPACTION_REPO, instanceId + "/compaction-job-execution");
        instanceProperties.set(ECR_INGEST_REPO, instanceId + "/ingest");
        instanceProperties.set(BULK_IMPORT_REPO, instanceId + "/bulk-import-runner");
        return instanceProperties;
    }

    private String getAccount() {
        return sts.getCallerIdentity(new GetCallerIdentityRequest()).getAccount();
    }

    public static final class Builder {
        private AmazonS3 s3;
        private AWSSecurityTokenService sts;
        private String instanceId;
        private String sleeperVersion;
        private String vpcId;
        private String subnetId;
        private Properties properties;
        private Properties tagsProperties;

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder sts(AWSSecurityTokenService sts) {
            this.sts = sts;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder sleeperVersion(String sleeperVersion) {
            this.sleeperVersion = sleeperVersion;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetId(String subnetId) {
            this.subnetId = subnetId;
            return this;
        }

        public Builder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder tagsProperties(Properties tagsProperties) {
            this.tagsProperties = tagsProperties;
            return this;
        }

        public Builder templatesDirectory(Path templatesDirectory) throws IOException {
            return sleeperVersion(Files.readString(templatesDirectory.resolve("version.txt")))
                    .properties(loadProperties(templatesDirectory.resolve("instanceproperties.template")))
                    .tagsProperties(loadProperties(templatesDirectory.resolve("tags.template")));
        }

        public GenerateInstanceProperties build() {
            return new GenerateInstanceProperties(this);
        }
    }
}
