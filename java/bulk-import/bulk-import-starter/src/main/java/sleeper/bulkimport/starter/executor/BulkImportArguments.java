/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.bulkimport.core.configuration.ConfigurationUtils;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.EmrInstanceArchitecture;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

public class BulkImportArguments {

    private final InstanceProperties instanceProperties;
    private final BulkImportJob bulkImportJob;
    private final String jobRunId;

    private BulkImportArguments(Builder builder) {
        instanceProperties = builder.instanceProperties;
        bulkImportJob = builder.bulkImportJob;
        jobRunId = builder.jobRunId;
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
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        String jobId = bulkImportJob.getId();
        return Stream.of(
                Stream.of("spark-submit", "--deploy-mode", "cluster"),
                sparkSubmitParameters(baseSparkConfig),
                Stream.of(jarLocation, configBucket, jobId, taskId, jobRunId, bulkImportMode))
                .flatMap(partialArgs -> partialArgs)
                .collect(Collectors.toUnmodifiableList());
    }

    public String sparkSubmitParametersForServerless() {
        return sparkSubmitParameters(
                ConfigurationUtils.getSparkServerlessConfigurationFromInstanceProperties(
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

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private BulkImportJob bulkImportJob;
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

        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        public BulkImportArguments build() {
            return new BulkImportArguments(this);
        }
    }
}
