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
package sleeper.core.deploy;

import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperPropertiesValidationReporter;

/**
 * A listener to be notified of validation failures for a Sleeper table configuration. Gathers failures to throw
 * as an exception.
 */
public class SleeperTableValidationReporter {

    private final SleeperPropertiesValidationReporter propertiesReporter = new SleeperPropertiesValidationReporter();
    private RuntimeException initialPartitionsFailure;

    public SleeperPropertiesValidationReporter getPropertiesReporter() {
        return propertiesReporter;
    }

    /**
     * Reports that the initial partition tree is invalid.
     *
     * @param failure the failure
     */
    public void initialPartitionsInvalid(RuntimeException failure) {
        initialPartitionsFailure = failure;
    }

    /**
     * Throws an exception if the table configuration was invalid.
     *
     * @throws SleeperPropertiesInvalidException if any table properties were reported as invalid
     * @throws InitialPartitionsInvalidException if the initial partition tree is invalid
     */
    public void throwIfFailed() throws SleeperPropertiesInvalidException {
        propertiesReporter.throwIfFailed();
        if (initialPartitionsFailure != null) {
            throw new InitialPartitionsInvalidException(initialPartitionsFailure);
        }
    }

}
