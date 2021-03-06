/*
 * Copyright 2022 Crown Copyright
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
package sleeper.systemtest;

import sleeper.configuration.properties.InstanceProperties;

/**
 * A class that extends {@link InstanceProperties} adding properties needed to
 * run the system tests that add random data to Sleeper.
 */
public class SystemTestProperties extends InstanceProperties {

    @Override
    protected void validate() {
        super.validate();
        for (SystemTestProperty systemTestProperty : SystemTestProperty.values()) {
            if (!systemTestProperty.validationPredicate().test(get(systemTestProperty))) {
                throw new IllegalArgumentException("sleeper property: " + systemTestProperty.getPropertyName() + " is invalid");
            }
        }
    }
}
