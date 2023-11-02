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

package sleeper.bulkimport.starter.executor;

import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.validation.EmrInstanceArchitecture;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

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

    public List<String> sparkSubmitCommandForCluster(String taskId, String jarLocation) {
        return sparkSubmitCommandForCluster(bulkImportJob, taskId, jarLocation);
    }

    public List<String> sparkSubmitCommandForCluster(BulkImportJob bulkImportJob, String taskId, String jarLocation) {
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        String jobId = bulkImportJob.getId();
        return Stream.of(
                        Stream.of("spark-submit", "--deploy-mode", "cluster"),
                        sparkSubmitArgs(bulkImportJob, Map.of()),
                        Stream.of(jarLocation, configBucket, jobId, taskId, jobRunId))
                .flatMap(partialArgs -> partialArgs)
                .collect(Collectors.toUnmodifiableList());
    }

    public String sparkSubmitParametersForServerless() {
        return sparkSubmitArgs(bulkImportJob,
                ConfigurationUtils.getSparkServerlessConfigurationFromInstanceProperties(
                        instanceProperties, EmrInstanceArchitecture.X86_64))
                .collect(Collectors.joining(" "));
    }

    private Stream<String> sparkSubmitArgs(
            BulkImportJob bulkImportJob, Map<String, String> baseSparkConf) {
        return Stream.concat(
                Stream.of("--class", getClassName(bulkImportJob)),
                mergeSparkConf(bulkImportJob, baseSparkConf)
                        .flatMap(entry -> Stream.of("--conf", entry.getKey() + "=" + entry.getValue())));
    }

    private Stream<Map.Entry<String, String>> mergeSparkConf(
            BulkImportJob bulkImportJob, Map<String, String> baseSparkConf) {
        Map<String, String> userConfig = bulkImportJob.getSparkConf();
        if (userConfig == null) {
            return baseSparkConf.entrySet().stream();
        }
        return Stream.concat(
                baseSparkConf.entrySet().stream(),
                readUserSparkConfRenamingSleeperProperties(userConfig));
    }

    private Stream<Map.Entry<String, String>> readUserSparkConfRenamingSleeperProperties(Map<String, String> userConfig) {
        return userConfig.entrySet()
                .stream().filter(prop -> prop.getKey().startsWith("sleeper.bulk.import.emr.serverless.spark"))
                .map(i -> Map.entry(
                        i.getKey().split("sleeper\\.bulk\\.import\\.emr\\.serverless\\.")[1],
                        i.getValue()));
    }

    private String getClassName(BulkImportJob bulkImportJob) {
        return bulkImportJob.getClassName() != null ? bulkImportJob.getClassName() : instanceProperties.get(BULK_IMPORT_CLASS_NAME);
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
