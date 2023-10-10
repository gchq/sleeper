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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;

import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;

public class EmrJarLocation {

    private EmrJarLocation() {
    }

    public static String getJarLocation(InstanceProperties instanceProperties) {
        return "s3a://"
                + instanceProperties.get(JARS_BUCKET)
                + "/bulk-import-runner-"
                + instanceProperties.get(CdkDefinedInstanceProperty.VERSION) + ".jar";
    }
}
