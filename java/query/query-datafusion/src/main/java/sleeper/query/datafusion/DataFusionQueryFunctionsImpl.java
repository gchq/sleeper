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
package sleeper.query.datafusion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.foreign.bridge.FFIBridge;

import java.io.IOException;
import java.util.Optional;

/**
 * Links to the DataFusion code for queries in Rust. This is done in a separate class to delay static
 * initialization until it is needed.
 */
public class DataFusionQueryFunctionsImpl {
    public static final Logger LOGGER = LoggerFactory.getLogger(DataFusionQueryFunctionsImpl.class);

    static final LoadFailureTracker INSTANCE = LoadFailureTracker.createForeignInterface();

    private DataFusionQueryFunctionsImpl() {
    }

    /**
     * A tracker for whether the DataFusion code failed to link.
     */
    static class LoadFailureTracker {

        private final DataFusionQueryFunctions functions;
        private final Exception failure;

        LoadFailureTracker(DataFusionQueryFunctions functions, Exception failure) {
            this.functions = functions;
            this.failure = failure;
        }

        private static LoadFailureTracker createForeignInterface() {
            try {
                DataFusionQueryFunctions functions = FFIBridge.createForeignInterface(DataFusionQueryFunctions.class);
                return new LoadFailureTracker(functions, null);
            } catch (RuntimeException | IOException e) {
                LOGGER.warn("Could not load foreign interface", e);
                return new LoadFailureTracker(null, e);
            }
        }

        DataFusionQueryFunctions getFunctionsOrThrow() {
            if (failure != null) {
                throw new IllegalStateException("Could not load foreign interface", failure);
            } else {
                return functions;
            }
        }

        Optional<DataFusionQueryFunctions> getFunctionsIfLoaded() {
            return Optional.ofNullable(functions);
        }
    }

}
