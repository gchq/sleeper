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
package sleeper.systemtest.drivers.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.dsl.snapshot.SnapshotsDriver;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class LocalStackSnapshotsDriver implements SnapshotsDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(LocalStackSnapshotsDriver.class);

    @Override
    public void enableCreation(InstanceProperties instanceProperties) {
        LOGGER.info("Requested enabling snapshots for instance {}, not currently implemented for LocalStack", instanceProperties.get(ID));
    }

    @Override
    public void disableCreation(InstanceProperties instanceProperties) {
        LOGGER.info("Requested disabling snapshots for instance {}, not currently implemented for LocalStack", instanceProperties.get(ID));
    }
}
