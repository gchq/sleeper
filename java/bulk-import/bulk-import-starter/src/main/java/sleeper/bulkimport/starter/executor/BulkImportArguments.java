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

import com.google.common.collect.Lists;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.instance.BulkImportProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

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

    public List<String> constructArgs(String taskId, String jarLocation) {
        return constructArgs(bulkImportJob, taskId, jarLocation);
    }

    public List<String> constructArgs(BulkImportJob bulkImportJob, String taskId) {
        Map<String, String> userConfig = bulkImportJob.getSparkConf();

        String className = bulkImportJob.getClassName() != null ? bulkImportJob.getClassName() : instanceProperties.get(BULK_IMPORT_CLASS_NAME);

        List<String> args = Lists.newArrayList("--class", className);

        if (null != userConfig) {
            for (Map.Entry<String, String> configurationItem : userConfig.entrySet()) {
                args.add("--conf");
                args.add(configurationItem.getKey() + "=" + configurationItem.getValue());
            }
        }
        return args;
    }

    public List<String> constructArgs(BulkImportJob bulkImportJob, String taskId, String jarLocation) {
        List<String> args = constructArgs(bulkImportJob, taskId);

        args.addAll(0, Lists.newArrayList("spark-submit", "--deploy-mode", "cluster"));
        args.add(jarLocation);
        args.add(instanceProperties.get(CONFIG_BUCKET));
        args.add(bulkImportJob.getId());
        args.add(taskId);
        args.add(jobRunId);
        return args;
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
