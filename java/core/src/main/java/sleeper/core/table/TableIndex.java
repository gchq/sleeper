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

package sleeper.core.table;

import java.util.Optional;
import java.util.stream.Stream;

public interface TableIndex {
    void create(TableIdentity tableId) throws TableAlreadyExistsException;

    Stream<TableIdentity> streamAllTables();

    Optional<TableIdentity> getTableByName(String tableName);

    Optional<TableIdentity> getTableByUniqueId(String tableUniqueId);

    void delete(TableIdentity tableId);

    void update(TableIdentity tableId);
}
