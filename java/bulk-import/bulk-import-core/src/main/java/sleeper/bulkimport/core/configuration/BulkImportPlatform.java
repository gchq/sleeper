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

public enum BulkImportPlatform {

    EMRServerless, NonPersistentEMR, PersistentEMR, EKS;

    public static BulkImportPlatform fromString(String value) {
        BulkImportPlatform platform = EnumUtils.getEnumIgnoreCase(BulkImportPlatform.class, value);
        if (platform == null) {
            throw new IllegalArgumentException("Invalid platform: " + value);
        }
        return platform;
    }

}
