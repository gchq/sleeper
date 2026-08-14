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

package sleeper.bulkimport.starter.executor;

import sleeper.bulkimport.core.configuration.SparkConfigurationUtils;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.EmrInstanceArchitecture;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class BulkImportArguments {

    private final InstanceProperties instanceProperties;
    private final BulkImportJob bulkImportJob;
    private final String jobFileObjectKey;
    private final String jobRunId;

    private BulkImportArguments(Builder builder) {
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        bulkImportJob = Objects.requireNonNull(builder.bulkImportJob, "bulkImportJob must not be null");
        jobFileObjectKey = Objects.requireNonNull(builder.jobFileObjectKey, "jobFileObjectKey must not be null");
        jobRunId = Objects.requireNonNull(builder.jobRunId, "jobRunId must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> sparkSubmitCommandForEMRCluster(String taskId, String jarLocation) {
        return sparkSubmitCommandForEMRCluster(taskId, jarLocation, Map.of());
    }

    public List<String> sparkSubmitCommandForEMRCluster(String taskId, String jarLocation, Map<String, String> baseSparkConfig) {
        return sparkSubmitCommandForCluster(taskId, jarLocation, baseSparkConfig, "EMR");
    }

    public List<String> sparkSubmitCommandForEKSCluster(String taskId, String jarLocation, Map<String, String> baseSparkConfig) {
        return sparkSubmitCommandForCluster(taskId, jarLocation, baseSparkConfig, "EKS");
    }

    private List<String> sparkSubmitCommandForCluster(String taskId, String jarLocation, Map<String, String> baseSparkConfig, String bulkImportMode) {
        return Stream.of(
                Stream.of("spark-submit", "--deploy-mode", "cluster"),
                sparkSubmitParameters(baseSparkConfig),
                Stream.of(jarLocation),
                Stream.of(entryPointArguments(taskId, bulkImportMode)))
                .flatMap(partialArgs -> partialArgs)
                .collect(Collectors.toUnmodifiableList());
    }

    public String[] entryPointArgumentsForServerless() {
        return entryPointArguments(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME) + "-EMRS", "EMR");
    }

    private String[] entryPointArguments(String taskId, String bulkImportMode) {
        return new String[]{instanceProperties.get(CONFIG_BUCKET), bulkImportJob.getId(), taskId, jobRunId, jobFileObjectKey, bulkImportMode};
    }

    public String sparkSubmitParametersForServerless() {
        return sparkSubmitParameters(
                SparkConfigurationUtils.getSparkServerlessConfigurationFromInstanceProperties(
                        instanceProperties, EmrInstanceArchitecture.X86_64))
                .collect(Collectors.joining(" "));
    }

    private Stream<String> sparkSubmitParameters(Map<String, String> baseSparkConfig) {
        return Stream.concat(
                Stream.of("--class", getClassName()),
                overrideWithUserSparkConfig(baseSparkConfig)
                        .flatMap(entry -> Stream.of("--conf", entry.getKey() + "=" + entry.getValue())));
    }

    private Stream<Map.Entry<String, String>> overrideWithUserSparkConfig(Map<String, String> baseSparkConfig) {
        Map<String, String> userConfig = bulkImportJob.getSparkConf();
        if (userConfig == null) {
            return baseSparkConfig.entrySet().stream();
        }
        return Stream.of(baseSparkConfig, userConfig)
                .flatMap(config -> config.keySet().stream())
                .distinct().flatMap(key -> mergeSparkValue(key, baseSparkConfig, userConfig)
                        .map(value -> entry(key, value))
                        .stream());
    }

    private static Optional<String> mergeSparkValue(
            String key, Map<String, String> baseConfig, Map<String, String> userConfig) {
        return Stream.of(userConfig, baseConfig)
                .map(config -> config.get(key))
                .filter(Objects::nonNull)
                .findFirst();
    }

    private String getClassName() {
        return bulkImportJob.getClassName() != null
                ? bulkImportJob.getClassName()
                : instanceProperties.get(BULK_IMPORT_CLASS_NAME);
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public BulkImportJob getBulkImportJob() {
        return bulkImportJob;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    @Override
    public String toString() {
        return "BulkImportArguments{instanceProperties=" + instanceProperties + ", bulkImportJob=" + bulkImportJob + ", jobFileObjectKey=" + jobFileObjectKey + ", jobRunId=" + jobRunId + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceProperties, bulkImportJob, jobFileObjectKey, jobRunId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BulkImportArguments)) {
            return false;
        }
        BulkImportArguments other = (BulkImportArguments) obj;
        return Objects.equals(instanceProperties, other.instanceProperties) && Objects.equals(bulkImportJob, other.bulkImportJob) && Objects.equals(jobFileObjectKey, other.jobFileObjectKey)
                && Objects.equals(jobRunId, other.jobRunId);
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private BulkImportJob bulkImportJob;
        private String jobFileObjectKey;
        private String jobRunId;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder bulkImportJob(BulkImportJob bulkImportJob) {
            this.bulkImportJob = bulkImportJob;
            return this;
        }

        public Builder jobFileObjectKey(String jobFileObjectKey) {
            this.jobFileObjectKey = jobFileObjectKey;
            return this;
        }

        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        public BulkImportArguments build() {
            return new BulkImportArguments(this);
        }
    }
}
