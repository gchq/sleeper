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

package sleeper.bulkexport.core.model;

import java.util.Optional;

/**
 * Custom exception for validation failure for a bulk export.
 */
public class BulkExportQueryValidationException extends RuntimeException {
    private final String exportId;

    public BulkExportQueryValidationException(String exportId, String message) {
        super(buildMessage(exportId, message));
        this.exportId = exportId;
    }

    public BulkExportQueryValidationException(String exportId, Exception cause) {
        super(buildMessage(exportId, cause.getMessage()), cause);
        this.exportId = exportId;
    }

    public Optional<String> getExportId() {
        return Optional.ofNullable(exportId);
    }

    private static String buildMessage(String exportId, String message) {
        if (exportId == null) {
            return "Query validation failed: " + message;
        } else {
            return "Query validation failed for export \"" + exportId + "\": " + message;
        }
    }
}
