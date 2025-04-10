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

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;

public enum BulkImportPlatform {

    EMRServerless, NonPersistentEMR, PersistentEMR, EKS;

    public String getBulkImportQueueUrl(InstanceProperties instanceProperties) {
        switch (this) {
            case EMRServerless:
                return instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL);
            case NonPersistentEMR:
                return instanceProperties.get(BULK_IMPORT_EMR_JOB_QUEUE_URL);
            case PersistentEMR:
                return instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL);
            case EKS:
                return instanceProperties.get(BULK_IMPORT_EKS_JOB_QUEUE_URL);
            default:
                throw new RuntimeException("Unexpected platform");
        }
    }

    public static BulkImportPlatform fromString(String value) {
        BulkImportPlatform platform = EnumUtils.getEnumIgnoreCase(BulkImportPlatform.class, value);
        if (platform == null) {
            throw new IllegalArgumentException("Invalid platform: " + value);
        }
        return platform;
    }

}
