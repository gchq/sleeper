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

package sleeper.core.properties.validation;

import org.apache.commons.lang3.EnumUtils;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

/**
 * Valid values to choose an ingest job queue. Each queue is for a different type of ingest or bulk import.
 */
public enum IngestQueue {
    STANDARD_INGEST(INGEST_JOB_QUEUE_URL),
    BULK_IMPORT_EMR(BULK_IMPORT_EMR_JOB_QUEUE_URL),
    BULK_IMPORT_PERSISTENT_EMR(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL),
    BULK_IMPORT_EKS(BULK_IMPORT_EKS_JOB_QUEUE_URL),
    BULK_IMPORT_EMR_SERVERLESS(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL);

    private final InstanceProperty jobQueueUrlProperty;

    IngestQueue(InstanceProperty jobQueueUrlProperty) {
        this.jobQueueUrlProperty = jobQueueUrlProperty;
    }

    /**
     * Checks if the value is a valid ingest job queue.
     *
     * @param  value the value
     * @return       true if it is valid
     */
    public static boolean isValid(String value) {
        return EnumUtils.isValidEnumIgnoreCase(IngestQueue.class, value);
    }

    /**
     * Retrieves the SQS URL of the ingest job queue from the instance properties.
     *
     * @param  instanceProperties the instance properties
     * @return                    the URL of the ingest job queue
     */
    public String getJobQueueUrl(InstanceProperties instanceProperties) {
        return instanceProperties.get(jobQueueUrlProperty);
    }
}
