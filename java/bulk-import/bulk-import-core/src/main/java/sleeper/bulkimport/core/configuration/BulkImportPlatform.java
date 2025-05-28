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
package sleeper.bulkimport.core.configuration;

import org.apache.commons.lang3.EnumUtils;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.model.OptionalStack;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

/**
 * The platforms a bulk import job can run on. These are different ways to run bulk import jobs with Spark.
 * Each one is deployed as an optional stack, which may or may not be enabled for a given Sleeper instance. A bulk
 * import job is always submitted for a specific platform, to a queue associated with that platform.
 * <p>
 * <strong>If you have occasional bulk import jobs, or you just want to get started, then we recommend EMR Serverless.
 * If you will have a lot of jobs running fairly constantly, then the persistent EMR approach is recommended.</strong>
 * <p>
 * Every platform deployment includes an SQS queue to submit bulk import jobs, and a lambda to receive jobs and start
 * them with Spark.
 * <p>
 * The {@link #EMRServerless} platform submits jobs to EMR Serverless. This is enabled by the optional stack
 * {@link OptionalStack#EmrServerlessBulkImportStack}. See configuration in
 * {@link sleeper.core.properties.instance.EMRServerlessProperty}.
 * <p>
 * The {@link #PersistentEMR} platform creates an EMR cluster when its optional stack is deployed, which all jobs are
 * submitted to. This is enabled by the optional stack {@link OptionalStack#PersistentEmrBulkImportStack}. See
 * configuration in {@link sleeper.core.properties.instance.PersistentEMRProperty}.
 * <p>
 * The {@link #NonPersistentEMR} platform creates an EMR cluster for each bulk import job, which terminates after the
 * job completes. This is enabled by the optional stack {@link OptionalStack#EmrBulkImportStack}. See configuration in
 * {@link sleeper.core.properties.instance.EMRProperty}.
 * <p>
 * The {@link #EKS} platform creates a Kubernetes cluster in EKS when its optional stack is deployed, and jobs are run
 * as Kubernetes tasks that drive Spark. This is enabled by the optional stack {@link OptionalStack#EksBulkImportStack}.
 * See configuration in {@link sleeper.core.properties.instance.EKSProperty}.
 */
public enum BulkImportPlatform {

    EMRServerless(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, OptionalStack.EmrServerlessBulkImportStack),
    NonPersistentEMR(BULK_IMPORT_EMR_JOB_QUEUE_URL, OptionalStack.EmrBulkImportStack),
    PersistentEMR(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, OptionalStack.PersistentEmrBulkImportStack),
    EKS(BULK_IMPORT_EKS_JOB_QUEUE_URL, OptionalStack.EksBulkImportStack);

    private final InstanceProperty queueUrlProperty;
    private final OptionalStack optionalStack;

    BulkImportPlatform(InstanceProperty queueUrlProperty, OptionalStack optionalStack) {
        this.queueUrlProperty = queueUrlProperty;
        this.optionalStack = optionalStack;
    }

    /**
     * Finds the URL of the SQS queue to submit bulk import jobs for this platform.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @return                    the SQS queue URL
     */
    public String getBulkImportQueueUrl(InstanceProperties instanceProperties) {
        return instanceProperties.get(queueUrlProperty);
    }

    /**
     * Retrieves the optional stack that enables this platform for a Sleeper instance.
     *
     * @return the optional stack
     */
    public OptionalStack getOptionalStack() {
        return optionalStack;
    }

    /**
     * Checks if this platform is deployed for a Sleeper instance.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @return                    true if the associated optional stack is enabled, false otherwise
     */
    public boolean isDeployed(InstanceProperties instanceProperties) {
        return instanceProperties.streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .anyMatch(optionalStack::equals);
    }

    public static BulkImportPlatform fromString(String value) {
        BulkImportPlatform platform = EnumUtils.getEnumIgnoreCase(BulkImportPlatform.class, value);
        if (platform == null) {
            throw new IllegalArgumentException("Invalid platform: " + value);
        }
        return platform;
    }

}
