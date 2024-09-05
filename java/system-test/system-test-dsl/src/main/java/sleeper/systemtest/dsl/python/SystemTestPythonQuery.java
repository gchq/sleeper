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

package sleeper.systemtest.dsl.python;

import sleeper.core.record.Record;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class SystemTestPythonQuery {
    private final SystemTestInstanceContext instance;
    private final PythonQueryTypesDriver driver;
    private final Path outputDir;
    private final List<String> queryIds = new ArrayList<>();

    public SystemTestPythonQuery(SystemTestContext context, Path outputDir) {
        this.instance = context.instance();
        this.driver = context.instance().adminDrivers().pythonQuery(context);
        this.outputDir = outputDir;
    }

    public SystemTestPythonQuery exactKeys(String keyName, String... keyValues) {
        String queryId = UUID.randomUUID().toString();
        driver.exactKeys(outputDir, queryId, keyName, List.of(keyValues));
        queryIds.add(queryId);
        return this;
    }

    public SystemTestPythonQuery range(String key, Object min, Object max) {
        return range(key, instance.getTableName(), min, max);
    }

    public SystemTestPythonQuery range(String key, String table, Object min, Object max) {
        String queryId = UUID.randomUUID().toString();
        driver.range(outputDir, queryId, key, table, min, max);
        queryIds.add(queryId);
        return this;
    }

    public SystemTestPythonQuery range(String key, Object min, boolean minInclusive, Object max, boolean maxInclusive) {
        String queryId = UUID.randomUUID().toString();
        driver.range(outputDir, queryId, key, min, minInclusive, max, maxInclusive);
        queryIds.add(queryId);
        return this;
    }

    public Stream<Record> results() {
        return queryIds.stream().flatMap(queryId -> driver.results(outputDir, queryId));
    }
}
