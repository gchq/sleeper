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
class DataFusionQueryFunctionsIfLoaded {
    public static final Logger LOGGER = LoggerFactory.getLogger(DataFusionQueryFunctionsIfLoaded.class);

    static final DataFusionQueryFunctionsIfLoaded INSTANCE = createForeignInterface();

    private final DataFusionQueryFunctions functions;
    private final Exception loadingFailure;

    private DataFusionQueryFunctionsIfLoaded(DataFusionQueryFunctions functions, Exception loadingFailure) {
        this.functions = functions;
        this.loadingFailure = loadingFailure;
    }

    private static DataFusionQueryFunctionsIfLoaded createForeignInterface() {
        try {
            DataFusionQueryFunctions functions = FFIBridge.createForeignInterface(DataFusionQueryFunctions.class);
            return new DataFusionQueryFunctionsIfLoaded(functions, null);
        } catch (RuntimeException | IOException e) {
            LOGGER.warn("Could not load foreign interface", e);
            return new DataFusionQueryFunctionsIfLoaded(null, e);
        }
    }

    DataFusionQueryFunctions getFunctionsOrThrow() {
        if (loadingFailure != null) {
            throw new IllegalStateException("Could not load foreign interface", loadingFailure);
        } else {
            return functions;
        }
    }

    Optional<DataFusionQueryFunctions> getFunctionsIfLoaded() {
        return Optional.ofNullable(functions);
    }

    Exception getLoadingFailure() {
        return loadingFailure;
    }
}
