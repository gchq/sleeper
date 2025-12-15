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

package sleeper.cdk.custom;

/**
 * Thrown to indicate that no table exists with the provided name when attempting to reuse a table during a create
 * action.
 */
public class NoTableToReuseException extends RuntimeException {
    public NoTableToReuseException(String tableName, Exception cause) {
        super("Table not found with name \"" + tableName + "\". If attempting to create a completly new table, " +
                "ensure the sleeper.table.reuse.existing property is set to false.", cause);
    }
}
