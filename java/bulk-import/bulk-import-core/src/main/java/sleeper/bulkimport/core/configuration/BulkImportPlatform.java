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

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;

public enum BulkImportPlatform {

    EMRServerless(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL),
    NonPersistentEMR(BULK_IMPORT_EMR_JOB_QUEUE_URL),
    PersistentEMR(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL),
    EKS(BULK_IMPORT_EKS_JOB_QUEUE_URL);

    private final InstanceProperty queueUrlProperty;

    BulkImportPlatform(InstanceProperty queueUrlProperty) {
        this.queueUrlProperty = queueUrlProperty;
    }

    public String getBulkImportQueueUrl(InstanceProperties instanceProperties) {
        return instanceProperties.get(queueUrlProperty);
    }

    public static BulkImportPlatform fromString(String value) {
        BulkImportPlatform platform = EnumUtils.getEnumIgnoreCase(BulkImportPlatform.class, value);
        if (platform == null) {
            throw new IllegalArgumentException("Invalid platform: " + value);
        }
        return platform;
    }

}
