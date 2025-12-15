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

package sleeper.core.table;

/**
 * Thrown to indicate that a table already exists with the given name. This can occur when creating or renaming a table.
 */
public class TableAlreadyExistsException extends RuntimeException {
    public TableAlreadyExistsException(TableStatus table) {
        this(table, null);
    }

    public TableAlreadyExistsException(TableStatus table, Exception cause) {
        super("Table already exists: " + table, cause);
    }

    public TableAlreadyExistsException(String message, Exception cause) {
        super(message, cause);
    }
}
